import mqtt from "mqtt";
import { ObjectId } from "mongodb";

let mqttClient = null;
let dbRefs = null;

// Orders are never handed to the board over MQTT any more. The board
// claims them itself through GET .../pending-orders (an atomic claim), so
// an order is only ever "dispensing" once the machine really has it. MQTT
// just tells the board "check now" so it doesn't wait for its next 5s
// poll; if that wake-up is lost, the poll still picks the order up.
//
// Previously the server marked the order "dispensing" and pushed the full
// order over MQTT. When that push was lost, the order sat "dispensing"
// (which the poll can't claim) until the 90s timeout failed it.
export async function dispatchNextPendingOrder(device) {
  if (!dbRefs || !dbRefs.ordersCollection) return false;
  try {
    const next = await dbRefs.ordersCollection.findOne(
      { deviceId: device._id.toString(), status: "pending" },
      { projection: { _id: 1 } }
    );
    if (!next) return false;
    return await publishWake(device.qrToken);
  } catch (err) {
    console.error("[queue] Error waking device for next order:", err);
    return false;
  }
}

export function initMqtt({ ordersCollection, devicesCollection, productsCollection, failOrderInternal }) {
  dbRefs = { ordersCollection, devicesCollection, productsCollection, failOrderInternal };

  const host = process.env.HIVEMQ_HOST;
  const port = Number(process.env.HIVEMQ_PORT || 8883);
  const username = process.env.HIVEMQ_USERNAME;
  const password = process.env.HIVEMQ_PASSWORD;

  if (!host || !username || !password) {
    console.warn("[mqtt] HiveMQ credentials not fully configured in .env, MQTT disabled");
    return null;
  }

  console.log(`[mqtt] Connecting to HiveMQ Cloud broker at ${host}:${port}...`);

  mqttClient = mqtt.connect({
    host,
    port,
    protocol: "mqtts",
    username,
    password,
    rejectUnauthorized: true,
    reconnectPeriod: 3000,
  });

  mqttClient.on("connect", () => {
    console.log("[mqtt] Connected to HiveMQ Cloud successfully!");

    const topics = [
      "devices/+/complete",
      "devices/+/progress",
      "devices/+/fail",
      "devices/+/telemetry",
    ];

    mqttClient.subscribe(topics, { qos: 1 }, (err) => {
      if (err) {
        console.error("[mqtt] Failed to subscribe to topics:", err);
      } else {
        console.log("[mqtt] Subscribed to device topics:", topics.join(", "));
      }
    });
  });

  mqttClient.on("error", (err) => {
    console.error("[mqtt] Error:", err.message);
  });

  mqttClient.on("offline", () => {
    console.warn("[mqtt] HiveMQ client offline, will reconnect...");
  });

  mqttClient.on("message", async (topic, messageBuffer) => {
    try {
      const parts = topic.split("/");
      if (parts.length < 3 || parts[0] !== "devices") return;

      const token = parts[1];
      const action = parts[2];
      const payload = JSON.parse(messageBuffer.toString());

      const device = await devicesCollection.findOne({ qrToken: token });
      if (!device) {
        console.warn(`[mqtt] Device with qrToken ${token} not found for message on ${topic}`);
        return;
      }

      if (action === "complete") {
        const { orderId } = payload;
        if (!orderId) return;

        const order = await ordersCollection.findOne({ _id: new ObjectId(orderId) });
        if (!order || order.deviceId !== device._id.toString()) return;

        // Guards against "failed" too, not just "completed" — a stale
        // order that already had its stock refunded by the timeout sweep
        // must not also be marked completed if the board eventually
        // reports in late, or the refund and the sale would both count.
        if (order.status !== "completed" && order.status !== "failed") {
          const fullyDispensedItems = order.items.map((item, i) => ({
            [`items.${i}.dispensedQty`]: item.qty,
          }));
          const dispensedFieldsSet = Object.assign({}, ...fullyDispensedItems);

          await ordersCollection.updateOne(
            { _id: order._id },
            { $set: { ...dispensedFieldsSet, status: "completed", completedAt: new Date() } }
          );
          console.log(`[mqtt] Order ${orderId} marked completed from device ${token}`);

          // FIFO: automatically pop and dispatch the next pending order in
          // line. Only when we actually completed something above — if
          // the order was already failed, whatever resolved it already
          // triggered the next dispatch.
          await dispatchNextPendingOrder(device);
        }
      } else if (action === "progress") {
        const { orderId, slotNumber } = payload;
        if (!orderId || slotNumber === undefined) return;

        const order = await ordersCollection.findOne({ _id: new ObjectId(orderId) });
        if (!order || order.deviceId !== device._id.toString()) return;

        // A progress report can arrive after the order was already
        // resolved — e.g. the board struggled to reconnect (weak WiFi)
        // long enough that the stale-order sweep timed it out and
        // restored stock before the delayed report caught up. Recording
        // dispensedQty on an already-failed/refunded order would make it
        // look like the item both got refunded AND dispensed, which is
        // exactly backwards from what actually happened.
        if (order.status === "completed" || order.status === "failed") {
          console.warn(`[mqtt] Order ${orderId} progress arrived after it was already ${order.status} — ignoring`);
          return;
        }

        const slot = Number(slotNumber);
        const itemIndex = order.items.findIndex((item) => item.slotNumber === slot);
        if (itemIndex === -1) return;

        const item = order.items[itemIndex];
        const dispensedQty = item.dispensedQty || 0;
        if (dispensedQty < item.qty) {
          const updatedQty = dispensedQty + 1;
          await ordersCollection.updateOne(
            { _id: order._id },
            { $set: { [`items.${itemIndex}.dispensedQty`]: updatedQty } }
          );
          console.log(`[mqtt] Order ${orderId} slot ${slot} progress: ${updatedQty}/${item.qty}`);
        }
      } else if (action === "fail") {
        const { orderId, reason } = payload;
        if (!orderId) return;

        const order = await ordersCollection.findOne({ _id: new ObjectId(orderId) });
        if (!order || order.deviceId !== device._id.toString()) return;
        if (order.status !== "completed" && order.status !== "failed") {
          // failOrderInternal dispatches the next queued order itself once
          // this one's marked failed — see index.js.
          await failOrderInternal(order, reason || "device_reported_failure");
          console.log(`[mqtt] Order ${orderId} marked failed: ${reason}`);
        }
      } else if (action === "telemetry") {
        const { ip, rssi, freeHeap, uptimeSeconds } = payload;
        await devicesCollection.updateOne(
          { _id: device._id },
          {
            $set: {
              lastSeen: new Date(),
              "telemetry.ip": ip || null,
              "telemetry.rssi": typeof rssi === "number" ? rssi : null,
              "telemetry.freeHeap": typeof freeHeap === "number" ? freeHeap : null,
              "telemetry.uptimeSeconds": typeof uptimeSeconds === "number" ? uptimeSeconds : null,
            },
          }
        );
      }
    } catch (err) {
      console.error("[mqtt] Failed to process incoming message:", err);
    }
  });

  return mqttClient;
}

// Tells the board to poll for pending orders right away. On a new topic
// ("wake", not the old "dispense"), so a board still running older
// firmware ignores it rather than dispensing from it.
export async function publishWake(token) {
  const host = process.env.HIVEMQ_HOST;
  const port = Number(process.env.HIVEMQ_PORT || 8883);
  const username = process.env.HIVEMQ_USERNAME;
  const password = process.env.HIVEMQ_PASSWORD;

  if (!host || !username || !password) {
    console.warn("[mqtt] HiveMQ credentials not configured in env, cannot publish");
    return false;
  }

  const topic = `devices/${token}/wake`;
  const message = "1";

  // Always a fresh, short-lived connection — never the shared mqttClient.
  // Vercel serverless functions freeze their entire event loop between
  // invocations, including the `mqtt` library's internal keepalive timer,
  // so a warm instance's shared client can sit with .connected still
  // reporting true long after HiveMQ has actually closed that session
  // server-side (it went quiet while frozen, so nothing ever noticed).
  // Publishing into that stale socket either fails silently or hangs — a
  // fresh connection here costs ~1-2s but is what's actually reliable.
  return new Promise((resolve) => {
    let settled = false;
    const client = mqtt.connect({
      host,
      port,
      protocol: "mqtts",
      username,
      password,
      reconnectPeriod: 0,
      connectTimeout: 5000,
    });

    // Backstop in case neither "connect" nor "error" ever fires for some
    // other reason — better to report failure (and let the caller revert
    // the order to pending) than hang the whole order-creation request.
    const hardTimeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      console.error(`[mqtt] Publish to ${topic} timed out with no connect/error event`);
      client.end(true);
      resolve(false);
    }, 7000);

    client.on("connect", () => {
      client.publish(topic, message, { qos: 1 }, (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(hardTimeout);
        if (err) {
          console.error(`[mqtt] Failed to publish wake-up to ${topic}:`, err);
        } else {
          console.log(`[mqtt] Published wake-up to ${topic}`);
        }
        client.end(true);
        resolve(!err);
      });
    });

    client.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimeout);
      console.error("[mqtt] Serverless publish connect error:", err.message);
      client.end(true);
      resolve(false);
    });
  });
}
