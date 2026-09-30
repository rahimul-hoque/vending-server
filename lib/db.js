import dotenv from "dotenv";
dotenv.config();

import { MongoClient, ServerApiVersion } from "mongodb";

// Single shared MongoClient for both better-auth and application queries.
// Previously auth.js and index.js each created their own MongoClient,
// doubling connection usage — a real problem on Vercel serverless where
// every cold start would open 2× connections against Atlas.
const client = new MongoClient(process.env.MONGO_URI, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
  // MongoDB's own recommendation for serverless (Vercel/Lambda): without
  // this, the driver will happily try to reuse a pooled connection that's
  // actually gone stale during a frozen/idle period between invocations,
  // which is what produces "MongoTopologyClosedError: Topology is closed"
  // on the next request. This makes it proactively discard idle
  // connections instead of trusting them indefinitely.
  maxIdleTimeMS: 60000,
  // This project runs on Atlas M0 (free tier) — a hard 500-connection
  // ceiling shared across the whole cluster. The driver's default
  // maxPoolSize is 100 *per client*, and Vercel can run several
  // concurrent serverless instances, each creating its own client at
  // module load — that combination can exhaust M0's connection limit on
  // its own, which Atlas then reports back as a closed/unreachable
  // topology. Capped low per MongoDB's own serverless guidance.
  maxPoolSize: 5,
});

const db = client.db("dispo");

export { client, db };
