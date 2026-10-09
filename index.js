import express, { json } from "express";
import cors from "cors";
import dotenv from "dotenv";
import { randomUUID } from "crypto";
import { ObjectId } from "mongodb";
import { fromNodeHeaders } from "better-auth/node";
import { client, db } from "./lib/db.js";
import { auth } from "./lib/auth.js";
import { initMqtt, publishOrderDispense, dispatchNextPendingOrder } from "./lib/mqtt.js";

dotenv.config();
const app = express();
const port = process.env.PORT;

app.use(json({ limit: "5mb" })); // product photos arrive as base64 data URLs — default 100kb limit would reject real images
app.use(cors());

async function run() {
  try {
    await client.connect();
    await client.db("admin").command({ ping: 1 });
    console.log(
      "Pinged your deployment. You successfully connected to MongoDB!"
    );

    // Compound indexes for the queries hit most often. createIndex() is
    // a no-op when the index already exists, so this is safe to run on
    // every cold start without duplicating work.
    await Promise.all([
      devicesCollection.createIndex({ qrToken: 1 }, { unique: true }),
      devicesCollection.createIndex({ ownerId: 1 }),
      productsCollection.createIndex({ deviceId: 1, slotNumber: 1 }, { unique: true }),
      ordersCollection.createIndex({ deviceId: 1, status: 1, createdAt: 1 }),
      ordersCollection.createIndex({ customerId: 1, createdAt: -1 }),
    ]);
    console.log("Database indexes ensured.");
  } finally {
    // Ensures that the client will close when you finish/error
    // await client.close();
  }
}
async function requireAuth(req, res, next) {
  let result;
  try {
    result = await auth.api.getSession({
      headers: fromNodeHeaders(req.headers),
    });
  } catch (error) {
    // A session lookup that *throws* (almost always a MongoDB hiccup) is
    // not the same as "not logged in" — answer with a JSON 503 instead of
    // an unhandled crash, so the client can show a retryable error and the
    // real cause lands in the logs.
    console.error("Session lookup failed:", error);
    return res.status(503).json({ error: "Auth service temporarily unavailable" });
  }
  if (!result) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  req.user = { id: result.user.id, role: result.user.role };
  next();
}

function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!allowedRoles.includes(req.user.role)) {
      return res
        .status(403)
        .json({ error: "Forbidden: Insufficient permissions" });
    }
    next();
  };
}

app.get("/api/whoami", requireAuth, (req, res) => {
  res.json(req.user);
});

const productsCollection = db.collection("products");
const devicesCollection = db.collection("devices");
const userCollection = db.collection("user");
const sessionCollection = db.collection("session");
const accountCollection = db.collection("account");
const ordersCollection = db.collection("orders");
const deviceLogsCollection = db.collection("device_logs");
const ALLOWED_ROLES = ["customer", "owner", "admin"];
const ALLOWED_DEVICE_TYPES = ["coffee_machine", "vending_machine", "juice_machine"];

run().catch(console.dir);

// An order stuck this long without the board confirming completion is
// treated as failed — either the machine never picked it up (still
// "pending") or it grabbed the order and never came back ("dispensing":
// jam, crash, power loss). Checked lazily on read (see expireStaleOrders)
// rather than a background cron job, since Vercel serverless has nowhere
// to run one.
//
// Set above the firmware's own ORDER_MAX_DURATION_MS (60s) so the board's
// own /fail report — which carries a specific reason — normally wins the
// race; this is just the backstop for a board that can't report at all
// (dead, powered off, network gone). Was 5 minutes — customers were
// staring at "dispensing your order" for up to 5 minutes with nothing
// telling them it had actually failed.
const STALE_ORDER_MS = 90 * 1000;

// Board polls every 5s and each poll stamps lastSeen (HEARTBEAT_WRITE_MS
// only stops back-to-back duplicate polls from writing twice). Offline =
// no poll for ONLINE_WINDOW_MS: about two missed polls plus slack for a
// slow serverless request. The board stops polling while it dispenses, so
// a device with an active order counts as online too.
const HEARTBEAT_WRITE_MS = 4 * 1000;
const ONLINE_WINDOW_MS = 15 * 1000;
const withOnline = (device, dispensingIds = new Set()) => ({
  ...device,
  online:
    dispensingIds.has(device._id.toString()) ||
    (!!device.lastSeen && Date.now() - new Date(device.lastSeen).getTime() < ONLINE_WINDOW_MS),
});

// Flips an order to "failed" and restores stock for whatever wasn't
// dispensed yet. `stockRestored` makes this idempotent — safe to call on
// the same order twice (e.g. the lazy sweep and an explicit /fail call
// racing each other) without crediting stock back more than once.
async function failOrderInternal(order, reason) {
  if (!order.stockRestored) {
    for (const item of order.items) {
      const remaining = item.qty - (item.dispensedQty || 0);
      if (remaining > 0) {
        await productsCollection.updateOne(
          { deviceId: order.deviceId, slotNumber: item.slotNumber },
          { $inc: { stock: remaining } }
        );
      }
    }
  }
  await ordersCollection.updateOne(
    { _id: order._id },
    { $set: { status: "failed", failedAt: new Date(), failureReason: reason, stockRestored: true } }
  );

  // A failed order frees up the device, so pop the next queued order (if
  // any). This is the one place shared by every failure path — the
  // device-reported /fail route, the MQTT fail handler, and the lazy
  // stale-order timeout sweep below — which matters because the sweep in
  // particular had no other way to unstick a queue stuck behind a machine
  // that went dark.
  const device = await devicesCollection.findOne({ _id: new ObjectId(order.deviceId) });
  if (device) {
    await dispatchNextPendingOrder(device);
  }
}

// Initialize HiveMQ Cloud MQTT connection and subscribers
initMqtt({ ordersCollection, devicesCollection, productsCollection, failOrderInternal });

// Called at the top of every order-reading route, scoped to whatever that
// route is already filtering by, so stale orders self-heal the moment
// anyone looks rather than needing a scheduled job.
async function expireStaleOrders(extraFilter = {}) {
  const cutoff = new Date(Date.now() - STALE_ORDER_MS);
  const staleOrders = await ordersCollection
    .find({
      ...extraFilter,
      $or: [
        { status: "pending", createdAt: { $lt: cutoff } },
        { status: "dispensing", dispensingStartedAt: { $lt: cutoff } },
      ],
    })
    .toArray();

  for (const order of staleOrders) {
    await failOrderInternal(order, "timeout");
  }
}

// ── Users (admin only) ─────────────────────────────────
app.get("/api/users", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    const users = await userCollection
      .find({}, { projection: { name: 1, email: 1, role: 1, phone: 1, createdAt: 1 } })
      .toArray();
    res.json(users);
  } catch (error) {
    console.error("Error listing users:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.patch("/api/users/:id/role", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    const { role } = req.body;
    if (!ALLOWED_ROLES.includes(role)) {
      return res.status(400).json({ error: `role must be one of: ${ALLOWED_ROLES.join(", ")}` });
    }
    const result = await userCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { role } }
    );
    if (result.matchedCount === 0) return res.status(404).json({ error: "User not found" });
    res.json({ success: true });
  } catch (error) {
    console.error("Error updating user role:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Permanent deletion, admin-only. Also cleans up the user's better-auth
// session/account records directly (we're bypassing better-auth's own API
// here since this is an admin acting on someone else's account), so no
// orphaned auth data is left sitting in the database. Devices/products/
// orders the user is connected to are deliberately left alone — cascading
// those deletes would be far more destructive than this feature asked for.
app.delete("/api/users/:id", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    if (req.params.id === req.user.id) {
      return res.status(400).json({ error: "You can't delete your own account." });
    }
    const user = await userCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!user) return res.status(404).json({ error: "User not found" });

    await userCollection.deleteOne({ _id: user._id });
    await sessionCollection.deleteMany({ userId: user._id.toString() });
    await accountCollection.deleteMany({ userId: user._id.toString() });

    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting user:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ── Devices ──────────────────────────────────────────────────────
// A device starts life "unclaimed": admin provisions a blank stub — just a
// permanent qrToken, no name/slots/owner yet — matching a real vending
// machine that ships from the factory with its QR code already printed on
// it. An owner later scans that same QR code to "claim" it: that's the
// moment name/slotCount/ownerId actually get set. The qrToken itself never
// changes across this whole lifecycle, so the physical sticker stays valid
// forever.

// Admin-only: mint a new blank device stub. Type and slot count are set
// here, up front, since a real physical machine's slot layout is fixed at
// manufacture time — the owner just gets shown this later, not asked to
// invent a number themselves. Normally generates a random token, but
// accepts an optional 'token' in the body so a real physical device that
// already shipped with its own ID (burned in by the manufacturer) can be
// registered under that exact ID instead of a mismatched random one.
// `confirmedAt` stays null until the ESP32 confirms (over Bluetooth) that
// it actually received and stored this token as its own device ID — the
// QR code isn't shown to the admin until that happens (see the
// provision-status route below). A custom-token device is assumed to
// already have its ID burned in some other way, so it's marked confirmed
// immediately and skips that step entirely.
app.post("/api/devices/provision", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    const customToken = req.body?.token?.trim();
    let qrToken = customToken || randomUUID();

    if (customToken) {
      const existing = await devicesCollection.findOne({ qrToken: customToken });
      if (existing) {
        return res.status(400).json({ error: "That token is already in use by another device." });
      }
    }

    const { deviceType, slotCount } = req.body || {};
    if (!ALLOWED_DEVICE_TYPES.includes(deviceType)) {
      return res.status(400).json({ error: `deviceType must be one of: ${ALLOWED_DEVICE_TYPES.join(", ")}` });
    }
    const parsedSlotCount = Number(slotCount);
    if (!Number.isInteger(parsedSlotCount) || parsedSlotCount < 1) {
      return res.status(400).json({ error: "Validation Error: 'slotCount' must be a positive integer" });
    }

    const newDevice = {
      qrToken,
      deviceType,
      slotCount: parsedSlotCount,
      name: null,
      ownerId: null,
      status: null, // becomes "active" once claimed
      createdAt: new Date(),
      claimedAt: null,
      confirmedAt: customToken ? new Date() : null,
    };
    const result = await devicesCollection.insertOne(newDevice);
    res.status(201).json({ _id: result.insertedId, ...newDevice });
  } catch (error) {
    console.error("Error provisioning device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Admin-only: called once the admin's browser confirms (via a Bluetooth
// notification from the ESP32) that the board actually received and
// stored its qrToken as its own device ID. This is what unlocks showing
// the printable QR code for a freshly-provisioned device.
app.patch("/api/devices/:id/provision-status", requireAuth, requireRole(["admin"]), async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!device) return res.status(404).json({ error: "Device not found" });
    await devicesCollection.updateOne({ _id: device._id }, { $set: { confirmedAt: new Date() } });
    res.json({ success: true });
  } catch (error) {
    console.error("Error confirming device provisioning:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Owner claims a previously-provisioned device by scanning its QR code —
// this is the first time it gets a name and an owner. Slot count is
// already set (by admin, at provisioning time) and only shown here, not
// asked for — though an explicit slotCount in the body is still honored,
// for backward compatibility with any device provisioned before this
// field existed.
app.post("/api/devices/:id/claim", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!device) return res.status(404).json({ error: "Device not found" });
    if (device.ownerId) {
      return res.status(400).json({ error: "This device has already been claimed." });
    }

    const { name, slotCount } = req.body;
    if (!name || name.trim() === "") {
      return res.status(400).json({ error: "Validation Error: 'name' is required" });
    }

    let parsedSlotCount = device.slotCount;
    if (slotCount !== undefined) {
      parsedSlotCount = Number(slotCount);
    }
    if (!Number.isInteger(parsedSlotCount) || parsedSlotCount < 1) {
      return res.status(400).json({ error: "Validation Error: this device has no slot count set — contact the admin" });
    }

    await devicesCollection.updateOne(
      { _id: device._id },
      {
        $set: {
          name: name.trim(),
          slotCount: parsedSlotCount,
          ownerId: req.user.id,
          status: "active",
          claimedAt: new Date(),
        },
      }
    );
    const updated = await devicesCollection.findOne({ _id: device._id });
    res.json(updated);
  } catch (error) {
    console.error("Error claiming device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Owner's own devices (or every device, for admin) — includes unclaimed
// stubs when the caller is admin, since that's how the admin device-
// management page shows what's ready to be handed to an owner.
app.get("/api/devices", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const filter = req.user.role === "admin" ? {} : { ownerId: req.user.id };
    const devices = await devicesCollection.find(filter).toArray();
    const dispensing = await ordersCollection
      .find(
        {
          deviceId: { $in: devices.map((d) => d._id.toString()) },
          status: "dispensing",
          dispensingStartedAt: { $gte: new Date(Date.now() - STALE_ORDER_MS) },
        },
        { projection: { deviceId: 1 } }
      )
      .toArray();
    const dispensingIds = new Set(dispensing.map((o) => o.deviceId));
    res.json(devices.map((d) => withOnline(d, dispensingIds)));
  } catch (error) {
    console.error("Error listing devices:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Public lookup — this is what a scanned QR code resolves through, for
// both the customer browsing flow and the owner claiming flow, before
// either of them has necessarily done anything yet.
app.get("/api/devices/by-token/:token", async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ qrToken: req.params.token });
    if (!device) return res.status(404).json({ error: "Device not found" });
    res.json(device);
  } catch (error) {
    console.error("Error resolving device token:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Public, minimal list for the manual "pick your machine" dropdown —
// deliberately excludes ownerId/qrToken, and only ever lists claimed,
// active devices (an unclaimed stub or a deliberately deactivated machine
// has nothing a customer should be able to pick).
app.get("/api/devices/public", async (req, res) => {
  try {
    const devices = await devicesCollection
      .find({ status: "active" }, { projection: { name: 1 } })
      .toArray();
    res.json(devices);
  } catch (error) {
    console.error("Error listing public devices:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Owner can rename, change slot count, or toggle active/inactive on a
// device they already claimed. qrToken itself is never editable here —
// see the note above on why it must stay permanent.
app.put("/api/devices/:id", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!device) return res.status(404).json({ error: "Not found" });
    if (device.ownerId !== req.user.id && req.user.role !== "admin") {
      return res.status(403).json({ error: "Not your device" });
    }

    const update = {};

    if (req.body.name !== undefined) {
      if (!req.body.name || req.body.name.trim() === "") {
        return res.status(400).json({ error: "Validation Error: 'name' can't be empty" });
      }
      update.name = req.body.name.trim();
    }

    if (req.body.status !== undefined) {
      if (!["active", "inactive"].includes(req.body.status)) {
        return res.status(400).json({ error: "Validation Error: status must be 'active' or 'inactive'" });
      }
      update.status = req.body.status;
    }

    if (req.body.slotCount !== undefined) {
      const parsedSlotCount = Number(req.body.slotCount);
      if (!Number.isInteger(parsedSlotCount) || parsedSlotCount < 1) {
        return res.status(400).json({ error: "Validation Error: 'slotCount' must be a positive integer" });
      }
      const highestUsedSlot = await productsCollection
        .find({ deviceId: device._id.toString() })
        .sort({ slotNumber: -1 })
        .limit(1)
        .toArray();
      if (highestUsedSlot.length > 0 && parsedSlotCount < highestUsedSlot[0].slotNumber) {
        return res.status(400).json({
          error: `Can't reduce to ${parsedSlotCount} slots — a product is already using slot ${highestUsedSlot[0].slotNumber}. Reassign or remove it first.`,
        });
      }
      update.slotCount = parsedSlotCount;
    }

    if (Object.keys(update).length === 0) {
      return res.status(400).json({ error: "Validation Error: nothing to update" });
    }

    await devicesCollection.updateOne({ _id: device._id }, { $set: update });
    const updated = await devicesCollection.findOne({ _id: device._id });
    res.json(updated);
  } catch (error) {
    console.error("Error updating device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/devices/:id", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!device) return res.status(404).json({ error: "Not found" });
    if (device.ownerId !== req.user.id && req.user.role !== "admin") {
      return res.status(403).json({ error: "Not your device" });
    }
    const productCount = await productsCollection.countDocuments({ deviceId: device._id.toString() });
    if (productCount > 0) {
      return res.status(400).json({
        error: `Can't delete — ${productCount} product(s) are still assigned to this device. Reassign or delete them first.`,
      });
    }
    await devicesCollection.deleteOne({ _id: device._id });
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Recorded by the owner-facing WiFi provisioning page (BLE feature) once
// the physical board actually confirms it joined a network. This is how
// /owner/devices shows real, verified WiFi status instead of a guess.
app.patch("/api/devices/:id/wifi-status", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!device) return res.status(404).json({ error: "Not found" });
    if (device.ownerId !== req.user.id && req.user.role !== "admin") {
      return res.status(403).json({ error: "Not your device" });
    }
    const { ip } = req.body;
    if (!ip) {
      return res.status(400).json({ error: "Validation Error: 'ip' is required" });
    }
    await devicesCollection.updateOne(
      { _id: device._id },
      { $set: { lastKnownIp: ip, wifiConfiguredAt: new Date() } }
    );
    res.json({ success: true });
  } catch (error) {
    console.error("Error recording device WiFi status:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ── Device-facing endpoints ───────────────────────────────────────
// Called directly by the ESP32 firmware itself, not by a logged-in user —
// there's no session/cookie to check here. Instead, the device proves it's
// legitimate simply by knowing its own qrToken, matching the same
// principle as the existing public by-token lookup above. This is a
// deliberate, pragmatic tradeoff for this project's scope: a real
// production system would likely use a dedicated device credential
// instead of reusing the QR token, but the QR token being printed on a
// physical sticker (rather than transmitted over the open internet
// unprompted) makes this a reasonable risk level here.

// Polled by the board every few seconds. Returns the single oldest
// pending order for this device, or {} if there's nothing to dispense —
// deliberately one order at a time rather than a full queue, so the
// firmware only ever has to think about one dispense sequence at once.
// Claims the order (→ "dispensing") in the same request it's handed out,
// so a board that grabs an order and then goes dark is distinguishable
// (stuck "dispensing") from one that's simply offline (order stays
// "pending" and nobody has claimed it).
app.get("/api/devices/by-token/:token/pending-orders", async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ qrToken: req.params.token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    // This poll doubles as the board's heartbeat. It's the one signal that
    // works reliably on Vercel (the MQTT subscriber there freezes between
    // invocations), so lastSeen is stamped here — throttled, to avoid a
    // database write on every 5s poll.
    if (!device.lastSeen || Date.now() - new Date(device.lastSeen).getTime() > HEARTBEAT_WRITE_MS) {
      await devicesCollection.updateOne({ _id: device._id }, { $set: { lastSeen: new Date() } });
    }

    await expireStaleOrders({ deviceId: device._id.toString() });

    // Atomic claim (findOneAndUpdate, not findOne+updateOne) — this route
    // is now polled periodically even while MQTT looks healthy (a backup
    // against a silently-failed publish), so it can run concurrently with
    // dispatchNextPendingOrder firing from an MQTT event. Without this
    // being atomic, both could grab the same pending order at once.
    const order = await ordersCollection.findOneAndUpdate(
      { deviceId: device._id.toString(), status: "pending" },
      { $set: { status: "dispensing", dispensingStartedAt: new Date() } },
      { sort: { createdAt: 1 }, returnDocument: "after" }
    );
    if (!order) return res.json({});

    res.json({
      orderId: order._id.toString(),
      items: order.items.map((item) => ({ slotNumber: item.slotNumber, qty: item.qty })),
    });
  } catch (error) {
    console.error("Error fetching pending orders for device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Called by the board after each individual unit it dispenses (not just
// once at the end) — this is what lets /fail restore only the portion
// that never actually left the machine, rather than the whole order.
// Capped at the ordered qty so a retried/duplicate call can't inflate it
// past what was really ordered.
app.patch("/api/devices/by-token/:token/orders/:orderId/items/:slotNumber/progress", async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ qrToken: req.params.token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    const order = await ordersCollection.findOne({ _id: new ObjectId(req.params.orderId) });
    if (!order) return res.status(404).json({ error: "Order not found" });
    if (order.deviceId !== device._id.toString()) {
      return res.status(403).json({ error: "That order doesn't belong to this device" });
    }

    // Same reasoning as the MQTT progress handler: a late report for an
    // order the stale sweep already failed (and refunded stock for)
    // would otherwise make it look both refunded and dispensed at once.
    if (order.status === "completed" || order.status === "failed") {
      return res.status(409).json({ error: `Order already ${order.status} — progress report ignored` });
    }

    const slotNumber = Number(req.params.slotNumber);
    const itemIndex = order.items.findIndex((item) => item.slotNumber === slotNumber);
    if (itemIndex === -1) {
      return res.status(400).json({ error: "That slot isn't part of this order" });
    }

    const item = order.items[itemIndex];
    const dispensedQty = item.dispensedQty || 0;
    if (dispensedQty >= item.qty) {
      return res.json({ success: true, dispensedQty }); // already fully reported — no-op
    }

    const updatedQty = dispensedQty + 1;
    await ordersCollection.updateOne(
      { _id: order._id },
      { $set: { [`items.${itemIndex}.dispensedQty`]: updatedQty } }
    );
    res.json({ success: true, dispensedQty: updatedQty });
  } catch (error) {
    console.error("Error recording dispense progress:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Called by the board once it's physically finished dispensing every item
// in an order. Verifies the order genuinely belongs to the device behind
// this token before completing it, so one board can't mark another
// device's orders complete even if it somehow guessed an order ID. Forces
// every item's dispensedQty to its full qty as a safety net, in case a
// /progress call was ever dropped along the way.
app.patch("/api/devices/by-token/:token/orders/:orderId/complete", async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ qrToken: req.params.token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    const order = await ordersCollection.findOne({ _id: new ObjectId(req.params.orderId) });
    if (!order) return res.status(404).json({ error: "Order not found" });
    if (order.deviceId !== device._id.toString()) {
      return res.status(403).json({ error: "That order doesn't belong to this device" });
    }
    if (order.status === "completed") {
      return res.json({ success: true }); // already done — treat as success, not an error
    }
    if (order.status === "failed") {
      return res.status(400).json({ error: "This order already timed out and had its stock restored." });
    }

    const fullyDispensedItems = order.items.map((item, i) => ({
      [`items.${i}.dispensedQty`]: item.qty,
    }));
    const dispensedFieldsSet = Object.assign({}, ...fullyDispensedItems);

    await ordersCollection.updateOne(
      { _id: order._id },
      { $set: { ...dispensedFieldsSet, status: "completed", completedAt: new Date() } }
    );
    res.json({ success: true });
  } catch (error) {
    console.error("Error completing order from device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Lets the board explicitly report "I couldn't finish this" (jam, sensor
// fault, out of stock in the physical hopper, etc.) instead of just going
// quiet and waiting for the timeout sweep to notice. Restores stock for
// whatever wasn't dispensed, same as the timeout path.
app.patch("/api/devices/by-token/:token/orders/:orderId/fail", async (req, res) => {
  try {
    const device = await devicesCollection.findOne({ qrToken: req.params.token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    const order = await ordersCollection.findOne({ _id: new ObjectId(req.params.orderId) });
    if (!order) return res.status(404).json({ error: "Order not found" });
    if (order.deviceId !== device._id.toString()) {
      return res.status(403).json({ error: "That order doesn't belong to this device" });
    }
    if (order.status === "completed" || order.status === "failed") {
      return res.json({ success: true }); // already resolved — treat as success
    }

    const reason = (req.body && req.body.reason) || "device_reported_failure";
    // failOrderInternal dispatches the next queued order itself.
    await failOrderInternal(order, reason);
    res.json({ success: true });
  } catch (error) {
    console.error("Error failing order from device:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Heartbeat, sent every ~30s by the firmware. Records the latest snapshot
// directly on the device document — this is "what's true right now", not
// a history, so it overwrites rather than appending.
app.post("/telemetry", async (req, res) => {
  try {
    const { deviceId: token, ip, rssi, freeHeap, uptimeSeconds } = req.body || {};
    if (!token) return res.status(400).json({ error: "Validation Error: 'deviceId' is required" });

    const device = await devicesCollection.findOne({ qrToken: token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    await devicesCollection.updateOne(
      { _id: device._id },
      {
        $set: {
          lastKnownIp: ip,
          lastTelemetryAt: new Date(),
          lastRssi: rssi,
          lastFreeHeap: freeHeap,
          lastUptimeSeconds: uptimeSeconds,
        },
      }
    );
    res.json({ success: true });
  } catch (error) {
    console.error("Error recording telemetry:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Dispense-completion log, sent once per physical dispense. Kept as its
// own append-only collection (unlike telemetry) since this is a real
// history worth preserving, not just a "latest state" snapshot.
app.post("/logs", async (req, res) => {
  try {
    const { deviceId: token, slotNumber, status, timestamp, rssi } = req.body || {};
    if (!token) return res.status(400).json({ error: "Validation Error: 'deviceId' is required" });

    const device = await devicesCollection.findOne({ qrToken: token });
    if (!device) return res.status(404).json({ error: "Device not found" });

    await deviceLogsCollection.insertOne({
      deviceId: device._id.toString(),
      slotNumber,
      status: status || "UNKNOWN",
      rssi,
      // Firmware sends epoch seconds (0 if NTP hasn't synced yet) — fall
      // back to server time when that happens, so the log entry still has
      // a sensible date rather than the 1970 epoch.
      deviceTimestamp: timestamp || null,
      createdAt: new Date(),
    });
    res.json({ success: true });
  } catch (error) {
    console.error("Error recording device log:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ── Products ─────────────────────────────────────────────────────
// Requires login (any role) — matches src/proxy.js on the Next.js side,
// which already gates every /shop/* page behind a session. Without
// requireAuth here, the full cross-tenant catalog (price, stock, images,
// ownerId) was reachable by anyone, logged in or not.
app.get(
  "/api/products",
  requireAuth,
  async (req, res) => {
    const filter = {};
    if (req.query.deviceId) {
      // Comma-separated list of device IDs is accepted alongside a single
      // ID (unchanged for existing callers) — lets a caller scope to
      // several owned devices in one request instead of fetching
      // everything and filtering client-side.
      const deviceIds = req.query.deviceId.split(",").map((id) => id.trim()).filter(Boolean);
      filter.deviceId = deviceIds.length > 1 ? { $in: deviceIds } : deviceIds[0];
    }
    // Lists never carry the photo itself — it's a base64 blob that dwarfs
    // the rest of the document. Clients get hasImage + imageVersion and load
    // the picture from /api/products/:id/image, which the browser caches.
    const products = await productsCollection
      .aggregate([{ $match: filter }, ...WITHOUT_IMAGE])
      .toArray();
    res.json(products);
  }
);

// The product photo as a real image response. imageVersion in the URL
// changes whenever the photo does, so the browser can cache it forever.
app.get("/api/products/:id/image", requireAuth, async (req, res) => {
  try {
    const product = await productsCollection.findOne(
      { _id: new ObjectId(req.params.id) },
      { projection: { image: 1 } }
    );
    const match = typeof product?.image === "string" && product.image.match(/^data:([^;,]+);base64,(.*)$/s);
    if (!match) return res.status(404).json({ error: "No image" });
    res.set("Content-Type", match[1]);
    res.set("Cache-Control", "private, max-age=31536000, immutable");
    res.send(Buffer.from(match[2], "base64"));
  } catch (error) {
    console.error("Error serving product image:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Single product lookup — backs the customer-facing product detail page,
// which itself sits behind a required session (src/proxy.js).
app.get("/api/products/:id", requireAuth, async (req, res) => {
  try {
    const [product] = await productsCollection
      .aggregate([{ $match: { _id: new ObjectId(req.params.id) } }, ...WITHOUT_IMAGE])
      .toArray();
    if (!product) return res.status(404).json({ error: "Product not found" });
    res.json(product);
  } catch (error) {
    console.error("Error fetching product:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Replaces the base64 `image` field with hasImage + imageVersion (0 for
// photos saved before versioning existed).
const WITHOUT_IMAGE = [
  {
    $addFields: {
      hasImage: { $and: [{ $eq: [{ $type: "$image" }, "string"] }, { $gt: [{ $strLenBytes: "$image" }, 0] }] },
      imageVersion: { $ifNull: ["$imageVersion", 0] },
    },
  },
  { $project: { image: 0 } },
];

// Shared by POST and PUT — a product's slot must be within the device's
// real slot count, and no other product on that same device may already
// occupy it (one physical slot holds exactly one product).
async function validateSlot(deviceId, slotNumber, excludeProductId) {
  const device = await devicesCollection.findOne({ _id: new ObjectId(deviceId) });
  if (!device) return "Validation Error: device not found";
  if (!device.slotCount) return "Validation Error: this device hasn't been claimed/configured yet";
  if (!Number.isInteger(slotNumber) || slotNumber < 1 || slotNumber > device.slotCount) {
    return `Validation Error: 'slotNumber' must be between 1 and ${device.slotCount} for this device`;
  }
  const conflictFilter = { deviceId, slotNumber };
  if (excludeProductId) conflictFilter._id = { $ne: new ObjectId(excludeProductId) };
  const conflict = await productsCollection.findOne(conflictFilter);
  if (conflict) return `Slot ${slotNumber} is already used by "${conflict.name}" on this device`;
  return null;
}

app.post(
  "/api/products",
  requireAuth,
  requireRole(["owner", "admin"]),
  async (req, res) => {
    try {
      const { name, description, price, stock, deviceId, image, slotNumber } = req.body;
      if (!name || name.trim() === "") {
        return res
          .status(400)
          .json({ error: "Validation Error: 'name' is required" });
      }

      if (!deviceId) {
        return res
          .status(400)
          .json({ error: "Validation Error: 'deviceId' is required" });
      }
      const device = await devicesCollection.findOne({ _id: new ObjectId(deviceId) });
      if (!device) {
        return res.status(400).json({ error: "Validation Error: device not found" });
      }
      if (device.ownerId !== req.user.id && req.user.role !== "admin") {
        return res.status(403).json({ error: "You don't own that device" });
      }

      const parsedSlotNumber = Number(slotNumber);
      const slotError = await validateSlot(deviceId, parsedSlotNumber, null);
      if (slotError) return res.status(400).json({ error: slotError });

      if (price === undefined || price === null) {
        return res
          .status(400)
          .json({ error: "Validation Error: 'price' is required" });
      }
      const parsedPrice = Number(price);
      if (isNaN(parsedPrice) || parsedPrice < 0) {
        return res
          .status(400)
          .json({
            error: "Validation Error: 'price' must be a valid positive number",
          });
      }
      let parsedStock = 0;
      if (stock !== undefined && stock !== null) {
        parsedStock = Number(stock);
        if (isNaN(parsedStock) || parsedStock < 0) {
          return res
            .status(400)
            .json({
              error:
                "Validation Error: 'stock' must be a valid positive integer",
            });
        }
      }
      const newProduct = {
        name: name.trim(),
        description: description ? description.trim() : "",
        price: parsedPrice, // Saved strictly as a Number for down-stream calculations
        stock: parsedStock, // Guaranteed to be a number (defaults to 0)
        image: image || null, // base64 data URL, or null if the owner skipped adding a photo
        imageVersion: Date.now(),
        slotNumber: parsedSlotNumber, // Which physical slot in the machine dispenses this item
        deviceId: deviceId, // Which physical machine this item is stocked in
        ownerId: req.user.id, // Securely injected from authentication middleware
        createdAt: new Date(),
      };

      const result = await productsCollection.insertOne(newProduct);

      const { image: _omit, ...withoutImage } = newProduct;
      return res.status(201).json({
        _id: result.insertedId,
        ...withoutImage,
        hasImage: !!newProduct.image,
      });
    } catch (error) {
      console.error("Error creating product:", error);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

app.put(
  "/api/products/:id",
  requireAuth,
  requireRole(["owner", "admin"]),
  async (req, res) => {
    try {
      const product = await productsCollection.findOne({ _id: new ObjectId(req.params.id) });
      if (!product) return res.status(404).json({ error: "Not found" });
      if (product.ownerId !== req.user.id && req.user.role !== "admin") {
        return res.status(403).json({ error: "Not your product" });
      }

      // Only these fields are editable — spreading req.body straight into
      // $set would let a caller overwrite ownerId, createdAt, etc.
      const update = {};
      for (const key of ["name", "description", "price", "stock", "slotNumber", "deviceId"]) {
        if (req.body[key] !== undefined) update[key] = req.body[key];
      }
      if (update.price !== undefined) update.price = Number(update.price);
      if (update.stock !== undefined) update.stock = Number(update.stock);
      if (update.deviceId !== undefined && update.deviceId !== product.deviceId) {
        const target = await devicesCollection.findOne({ _id: new ObjectId(update.deviceId) });
        if (!target) return res.status(400).json({ error: "Validation Error: device not found" });
        if (target.ownerId !== req.user.id && req.user.role !== "admin") {
          return res.status(403).json({ error: "You don't own that device" });
        }
      }
      // `image` omitted = keep the current photo; a data URL replaces it;
      // null removes it. Bumping imageVersion busts the browser cache.
      if (req.body.image !== undefined) {
        update.image = req.body.image || null;
        update.imageVersion = Date.now();
      }
      if (update.slotNumber !== undefined) {
        const parsedSlotNumber = Number(update.slotNumber);
        const targetDeviceId = update.deviceId || product.deviceId;
        const slotError = await validateSlot(targetDeviceId, parsedSlotNumber, product._id.toString());
        if (slotError) return res.status(400).json({ error: slotError });
        update.slotNumber = parsedSlotNumber;
      }

      await productsCollection.updateOne({ _id: product._id }, { $set: update });
      res.json({
        success: true,
        hasImage: update.image !== undefined ? !!update.image : !!product.image,
        imageVersion: update.imageVersion ?? product.imageVersion ?? 0,
      });
    } catch (error) {
      console.error("Error updating product:", error);
      res.status(500).json({ error: "Internal server error" });
    }
  }
);

app.delete(
  "/api/products/:id",
  requireAuth,
  requireRole(["admin", "owner"]),
  async (req, res) => {
    try {
      const product = await productsCollection.findOne({_id: new ObjectId(req.params.id)})
      if (!product) return res.status(404).json({ error: "Not found" });
      if (product.ownerId !== req.user.id && req.user.role !== "admin") {
        return res.status(403).json({ error: "Not your product" });
      }
      await productsCollection.deleteOne({_id: product._id});
      res.json({success: true});
    } catch (error) {
      console.error("Error deleting product:", error);
      res.status(500).json({ error: "Internal server error" });
    }
  }
)

// ── Orders ──────────────────────────────────────────────────────
// Any logged-in customer can place an order. Payment is simulated (the
// TODO below is where a real Stripe sandbox integration would slot in),
// but the dispense lifecycle is real: every order starts "pending" and
// only becomes "completed" once the physical machine actually dispenses
// the item and confirms it. For now (no real ESP32 endpoint wired up
// yet), the owner can mark an order complete manually from /owner/orders —
// that's the same transition a real hardware callback would trigger later.
app.post("/api/orders", requireAuth, async (req, res) => {
  try {
    const { deviceId, items } = req.body;

    if (!deviceId) {
      return res.status(400).json({ error: "Validation Error: 'deviceId' is required" });
    }
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: "Validation Error: cart is empty" });
    }

    const device = await devicesCollection.findOne({ _id: new ObjectId(deviceId) });
    if (!device) {
      return res.status(400).json({ error: "Validation Error: device not found" });
    }
    if (device.status !== "active") {
      return res.status(403).json({
        error: "This machine is currently inactive and isn't accepting orders.",
        code: "DEVICE_INACTIVE",
      });
    }

    // Re-fetch every product server-side by (deviceId, slotNumber) — never
    // trust price/name/qty sent from the browser, and slotNumber is what
    // the physical machine actually needs to know what to dispense.
    //
    // Stock is checked and decremented in the SAME atomic findOneAndUpdate
    // per item (the stock: {$gte: qty} guard lives in the filter, not a
    // separate read beforehand) — two concurrent checkouts racing for the
    // last unit can't both pass a plain read-then-decrement and oversell,
    // because only one of them will ever match the filter for that final
    // unit. If a later item in the same cart fails, everything this order
    // already decremented gets rolled back below.
    const orderItems = [];
    const decremented = [];
    let total = 0;
    let validationError = null;

    for (const requested of items) {
      const slotNumber = Number(requested.slotNumber);
      const qty = Number(requested.qty);
      if (!Number.isInteger(slotNumber) || !qty || qty < 1) {
        validationError = { status: 400, message: "Validation Error: each item needs a slotNumber and qty >= 1" };
        break;
      }

      const claimed = await productsCollection.findOneAndUpdate(
        { deviceId, slotNumber, stock: { $gte: qty } },
        { $inc: { stock: -qty } },
        { returnDocument: "after" }
      );

      if (!claimed) {
        const product = await productsCollection.findOne({ deviceId, slotNumber });
        validationError = product
          ? { status: 400, message: `Not enough stock for ${product.name} (only ${product.stock} left)` }
          : { status: 400, message: `No product found in slot ${slotNumber} on this device` };
        break;
      }

      decremented.push({ slotNumber, qty });
      total += claimed.price * qty;
      orderItems.push({
        slotNumber,
        name: claimed.name, // locked in at time of purchase
        price: claimed.price, // locked in at time of purchase
        qty,
        dispensedQty: 0,
      });
    }

    if (validationError) {
      // Give back whatever this same order already took before the item
      // that failed, so a rejected cart doesn't leave stock short.
      for (const item of decremented) {
        await productsCollection.updateOne(
          { deviceId, slotNumber: item.slotNumber },
          { $inc: { stock: item.qty } }
        );
      }
      return res.status(validationError.status).json({ error: validationError.message });
    }

    const activeOrder = await ordersCollection.findOne({
      deviceId,
      status: "dispensing",
    });

    const isIdle = !activeOrder;

    const newOrder = {
      customerId: req.user.id,
      deviceId,
      items: orderItems,
      total,
      status: isIdle ? "dispensing" : "pending",
      createdAt: new Date(),
      dispensingStartedAt: isIdle ? new Date() : null,
      completedAt: null,
      failedAt: null,
      failureReason: null,
      stockRestored: false,
    };
    const result = await ordersCollection.insertOne(newOrder);

    // If machine is idle, trigger hardware immediately via HiveMQ. The
    // order was already marked "dispensing" above — if this publish
    // fails (or we can't even confirm it succeeded), that claim was a
    // lie: nothing was actually sent, and the order would otherwise sit
    // there "dispensing" with no way to recover except waiting out the
    // full stale-order timeout. Revert it to "pending" so the board's
    // own periodic backup poll (or the next order's completion, via
    // dispatchNextPendingOrder) picks it up and retries — instead of
    // silently stranding it.
    if (isIdle) {
      let publishedOk = false;
      try {
        const device = await devicesCollection.findOne({ _id: new ObjectId(deviceId) });
        if (device?.qrToken) {
          publishedOk = await publishOrderDispense(device.qrToken, {
            orderId: result.insertedId.toString(),
            items: newOrder.items.map((i) => ({ slotNumber: i.slotNumber, qty: i.qty })),
          });
        }
      } catch (mqttErr) {
        console.error("[mqtt] Error publishing order trigger:", mqttErr);
      }

      if (!publishedOk) {
        console.warn(`[mqtt] Publish failed or unconfirmed for order ${result.insertedId} — reverting to pending for retry`);
        await ordersCollection.updateOne(
          { _id: result.insertedId },
          { $set: { status: "pending", dispensingStartedAt: null } }
        );
        newOrder.status = "pending";
        newOrder.dispensingStartedAt = null;
      }
    }

    res.status(201).json({ _id: result.insertedId, ...newOrder });
  } catch (error) {
    console.error("Error creating order:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Current customer's own order history
app.get("/api/orders/mine", requireAuth, async (req, res) => {
  try {
    const filter = { customerId: req.user.id };
    await expireStaleOrders(filter);
    const orders = await ordersCollection.find(filter).sort({ createdAt: -1 }).toArray();
    res.json(orders);
  } catch (error) {
    console.error("Error listing orders:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Single order, scoped to whoever's allowed to see it — the customer who
// placed it, the owner of the device it's on, or admin. Meant to be
// polled by the checkout confirmation screen so the customer can watch
// pending -> dispensing -> completed happen live.
app.get("/api/orders/:id", requireAuth, async (req, res) => {
  try {
    const order = await ordersCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!order) return res.status(404).json({ error: "Order not found" });

    if (req.user.role !== "admin" && order.customerId !== req.user.id) {
      const device = await devicesCollection.findOne({ _id: new ObjectId(order.deviceId) });
      if (!device || device.ownerId !== req.user.id) {
        return res.status(403).json({ error: "Not your order" });
      }
    }

    await expireStaleOrders({ _id: order._id });
    const fresh = await ordersCollection.findOne({ _id: order._id });
    res.json(fresh);
  } catch (error) {
    console.error("Error fetching order:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Orders an owner may see: those on their own devices. Admin sees all.
async function orderScopeFilter(user) {
  if (user.role === "admin") return {};
  const ownDevices = await devicesCollection
    .find({ ownerId: user.id }, { projection: { _id: 1 } })
    .toArray();
  return { deviceId: { $in: ownDevices.map((d) => d._id.toString()) } };
}

// Owner sees orders across their own devices; admin sees every order.
// Paginated newest-first: ?limit (default 20, max 100) and ?before=<order
// _id> from the previous page's nextCursor. Optional ?status and ?deviceId
// filters run server-side so pagination stays correct when filtering.
app.get("/api/orders", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const scope = await orderScopeFilter(req.user);
    await expireStaleOrders(scope);

    const filter = { ...scope };
    if (["pending", "dispensing", "completed", "failed"].includes(req.query.status)) {
      filter.status = req.query.status;
    }
    if (req.query.deviceId) {
      const allowed = !scope.deviceId || scope.deviceId.$in.includes(req.query.deviceId);
      filter.deviceId = allowed ? req.query.deviceId : "__none__";
    }
    if (req.query.before && ObjectId.isValid(req.query.before)) {
      filter._id = { $lt: new ObjectId(req.query.before) };
    }
    const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 100);

    const orders = await ordersCollection.find(filter).sort({ _id: -1 }).limit(limit + 1).toArray();
    const hasMore = orders.length > limit;
    const page = hasMore ? orders.slice(0, limit) : orders;
    res.json({ orders: page, nextCursor: hasMore ? page[page.length - 1]._id.toString() : null });
  } catch (error) {
    console.error("Error listing all orders:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Dashboard numbers computed in the database instead of shipping every
// order to the browser. Failed orders had their stock refunded, so they
// don't count towards revenue or top sellers. ?tz (IANA name) sets which
// calendar day an order falls on.
app.get("/api/orders/stats", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const scope = await orderScopeFilter(req.user);
    await expireStaleOrders(scope);

    let timezone = "UTC";
    try {
      if (req.query.tz) {
        new Intl.DateTimeFormat("en", { timeZone: req.query.tz });
        timezone = req.query.tz;
      }
    } catch {} // unknown zone name: fall back to UTC

    const [result] = await ordersCollection
      .aggregate([
        { $match: scope },
        {
          $facet: {
            byStatus: [{ $group: { _id: "$status", count: { $sum: 1 } } }],
            revenue: [
              { $match: { status: { $ne: "failed" } } },
              { $group: { _id: null, total: { $sum: "$total" } } },
            ],
            revenueByDay: [
              { $match: { status: { $ne: "failed" } } },
              {
                $group: {
                  _id: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt", timezone } },
                  revenue: { $sum: "$total" },
                },
              },
              { $sort: { _id: 1 } },
              { $project: { _id: 0, day: "$_id", revenue: 1 } },
            ],
            topSelling: [
              { $match: { status: { $ne: "failed" } } },
              { $unwind: "$items" },
              { $group: { _id: "$items.name", qty: { $sum: "$items.qty" } } },
              { $sort: { qty: -1 } },
              { $limit: 8 },
              { $project: { _id: 0, name: "$_id", qty: 1 } },
            ],
          },
        },
      ])
      .toArray();

    const counts = { pending: 0, dispensing: 0, completed: 0, failed: 0 };
    for (const { _id, count } of result.byStatus) if (_id in counts) counts[_id] = count;

    res.json({
      totalRevenue: result.revenue[0]?.total || 0,
      orderCount: counts.pending + counts.dispensing + counts.completed + counts.failed,
      counts,
      revenueByDay: result.revenueByDay,
      topSelling: result.topSelling,
    });
  } catch (error) {
    console.error("Error computing order stats:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Manual "mark as dispensed" for now — this is exactly the transition a
// real ESP32 dispense-confirmation callback would trigger later; the data
// shape doesn't need to change when that gets built, only who calls this.
app.patch("/api/orders/:id/complete", requireAuth, requireRole(["owner", "admin"]), async (req, res) => {
  try {
    const order = await ordersCollection.findOne({ _id: new ObjectId(req.params.id) });
    if (!order) return res.status(404).json({ error: "Order not found" });

    const device = await devicesCollection.findOne({ _id: new ObjectId(order.deviceId) });
    if (req.user.role !== "admin") {
      if (!device || device.ownerId !== req.user.id) {
        return res.status(403).json({ error: "Not your order" });
      }
    }

    if (order.status === "completed") {
      return res.status(400).json({ error: "Order is already completed" });
    }
    if (order.status === "failed") {
      return res.status(400).json({ error: "This order already timed out and had its stock restored." });
    }

    const fullyDispensedItems = order.items.map((item, i) => ({
      [`items.${i}.dispensedQty`]: item.qty,
    }));
    const dispensedFieldsSet = Object.assign({}, ...fullyDispensedItems);

    await ordersCollection.updateOne(
      { _id: order._id },
      { $set: { ...dispensedFieldsSet, status: "completed", completedAt: new Date() } }
    );

    // Same as the device/MQTT completion paths — free up the device for
    // whatever's queued behind this order.
    if (device) {
      await dispatchNextPendingOrder(device);
    }
    res.json({ success: true });
  } catch (error) {
    console.error("Error completing order:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/", (req, res) => {
  res.send("SERVER created");
});

// Only actually binds to a port for local dev (`node index.js` /
// `nodemon index.js`). On Vercel, the exported `app` below is what gets
// used directly as a serverless request handler — this listen() call
// simply never gets reached in that environment.
if (!process.env.VERCEL) {
  app.listen(port, () => {
    console.log(`server running at port: ${port}`);
  });
}

export default app;
