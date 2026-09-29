import mqtt from "mqtt";
import { ObjectId } from "mongodb";

let mqttClient = null;
let dbRefs = null;

export async function dispatchNextPendingOrder(device) {
  if (!dbRefs || !dbRefs.ordersCollection) return null;
  const { ordersCollection } = dbRefs;

  try {
    const nextOrder = await ordersCollection.findOne(
      { deviceId: device._id.toString(), status: "pending" },
      { sort: { createdAt: 1 } } // FIFO: Oldest order first
    );

    if (nextOrder) {
      await ordersCollection.updateOne(
        { _id: nextOrder._id },
        { $set: { status: "dispensing", dispensingStartedAt: new Date() } }
      );

      console.log(`[queue] Auto-dispatching queued order ${nextOrder._id} to device ${device.qrToken}`);
      publishOrderDispense(device.qrToken, {
        orderId: nextOrder._id.toString(),
        items: nextOrder.items.map((i) => ({ slotNumber: i.slotNumber, qty: i.qty })),
      });
      return nextOrder;
    }
  } catch (err) {
    console.error("[queue] Error dispatching next pending order:", err);
  }
  return null;
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

        if (order.status !== "completed") {
          const fullyDispensedItems = order.items.map((item, i) => ({
            [`items.${i}.dispensedQty`]: item.qty,
          }));
          const dispensedFieldsSet = Object.assign({}, ...fullyDispensedItems);

          await ordersCollection.updateOne(
            { _id: order._id },
            { $set: { ...dispensedFieldsSet, status: "completed", completedAt: new Date() } }
          );
          console.log(`[mqtt] Order ${orderId} marked completed from device ${token}`);
        }

        // FIFO: Automatically pop and dispatch the next pending order in line!
        await dispatchNextPendingOrder(device);
      } else if (action === "progress") {
        const { orderId, slotNumber } = payload;
        if (!orderId || slotNumber === undefined) return;

        const order = await ordersCollection.findOne({ _id: new ObjectId(orderId) });
        if (!order || order.deviceId !== device._id.toString()) return;

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
          await failOrderInternal(order, reason || "device_reported_failure");
          console.log(`[mqtt] Order ${orderId} marked failed: ${reason}`);
        }

        // FIFO: If current order failed, move on to the next pending order!
        await dispatchNextPendingOrder(device);
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

export async function publishOrderDispense(token, orderData) {
  const host = process.env.HIVEMQ_HOST;
  const port = Number(process.env.HIVEMQ_PORT || 8883);
  const username = process.env.HIVEMQ_USERNAME;
  const password = process.env.HIVEMQ_PASSWORD;

  if (!host || !username || !password) {
    console.warn("[mqtt] HiveMQ credentials not configured in env, cannot publish");
    return false;
  }

  const topic = `devices/${token}/dispense`;
  const message = JSON.stringify(orderData);

  if (mqttClient && mqttClient.connected) {
    return new Promise((resolve) => {
      mqttClient.publish(topic, message, { qos: 1 }, (err) => {
        if (err) {
          console.error(`[mqtt] Failed to publish dispense command to ${topic}:`, err);
        } else {
          console.log(`[mqtt] Published dispense command to ${topic} for order ${orderData.orderId}`);
        }
        resolve(!err);
      });
    });
  }

  // Serverless cold-start: create dedicated connection, await connect & publish
  return new Promise((resolve) => {
    const client = mqtt.connect({
      host,
      port,
      protocol: "mqtts",
      username,
      password,
      reconnectPeriod: 0,
      connectTimeout: 5000,
    });

    client.on("connect", () => {
      client.publish(topic, message, { qos: 1 }, (err) => {
        if (err) {
          console.error(`[mqtt] Failed to publish dispense command to ${topic}:`, err);
        } else {
          console.log(`[mqtt] Published dispense command to ${topic} for order ${orderData.orderId}`);
        }
        client.end(true);
        resolve(!err);
      });
    });

    client.on("error", (err) => {
      console.error("[mqtt] Serverless publish connect error:", err.message);
      client.end(true);
      resolve(false);
    });
  });
}
