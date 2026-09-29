// Fakes an ESP32 board over MQTT so the whole order/dispense pipeline can
// be tested against real HiveMQ + MongoDB without flashing hardware.
// Subscribes to the same topic the firmware will, and responds on
// devices/<token>/progress|complete|fail exactly like the real board.
//
// Usage:
//   node scripts/simulate-device.js <qrToken> [complete|fail|silent]
//
//   complete (default) — dispense every unit one at a time, then report done
//   fail               — dispense nothing, immediately report a failure
//   silent             — never respond at all (simulates a dead/offline board,
//                         so you can watch the 5-minute stale-order sweep
//                         auto-fail it and restore stock)

import mqtt from "mqtt";
import dotenv from "dotenv";
dotenv.config();

const token = process.argv[2];
const mode = process.argv[3] || "complete";

if (!token) {
  console.error("Usage: node scripts/simulate-device.js <qrToken> [complete|fail|silent]");
  process.exit(1);
}
if (!["complete", "fail", "silent"].includes(mode)) {
  console.error(`Unknown mode "${mode}" — use complete, fail, or silent`);
  process.exit(1);
}

const host = process.env.HIVEMQ_HOST;
const port = Number(process.env.HIVEMQ_PORT || 8883);
const username = process.env.HIVEMQ_USERNAME;
const password = process.env.HIVEMQ_PASSWORD;

if (!host || !username || !password) {
  console.error("Missing HIVEMQ_HOST/PORT/USERNAME/PASSWORD — check dispo-server/.env");
  process.exit(1);
}

const PULSE_MS = 500; // pretend a physical relay pulse takes this long, same as RELAY_PULSE_MS on the real board

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const client = mqtt.connect({ host, port, protocol: "mqtts", username, password });

function publish(topic, payload) {
  client.publish(topic, JSON.stringify(payload), { qos: 1 });
}

client.on("connect", () => {
  console.log(`[sim] connected to HiveMQ as fake device "${token}" — mode=${mode}`);
  client.subscribe(`devices/${token}/dispense`, { qos: 1 }, (err) => {
    if (err) {
      console.error("[sim] subscribe failed:", err.message);
      process.exit(1);
    }
    console.log(`[sim] listening on devices/${token}/dispense — place an order against this device now\n`);
  });
});

client.on("error", (err) => console.error("[sim] connection error:", err.message));

client.on("message", async (topic, buf) => {
  let payload;
  try {
    payload = JSON.parse(buf.toString());
  } catch {
    console.error("[sim] got non-JSON message, ignoring:", buf.toString());
    return;
  }

  const { orderId, items } = payload;
  console.log(`[sim] dispense command for order ${orderId}:`, items);

  if (mode === "silent") {
    console.log("[sim] silent mode — not responding. Check back in ~5 minutes; the order should auto-fail and stock should be restored.\n");
    return;
  }

  if (mode === "fail") {
    await sleep(800);
    publish(`devices/${token}/fail`, { orderId, reason: "simulated_jam" });
    console.log(`[sim] reported /fail for order ${orderId}\n`);
    return;
  }

  for (const item of items) {
    for (let unit = 1; unit <= item.qty; unit++) {
      await sleep(PULSE_MS);
      publish(`devices/${token}/progress`, { orderId, slotNumber: item.slotNumber });
      console.log(`[sim]   slot ${item.slotNumber}: dispensed unit ${unit}/${item.qty}`);
    }
  }

  await sleep(300);
  publish(`devices/${token}/complete`, { orderId });
  console.log(`[sim] order ${orderId} complete\n`);
});
