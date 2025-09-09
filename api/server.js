// api/server.js
import express from "express";
import cors from "cors";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

dotenv.config({ path: "./.env" });

const app = express();
app.use(cors());
app.use(express.json());

const MONGODB_URI = process.env.MONGODB_URI || process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB || "bdg";
const COLL = process.env.MONGO_COLLECTION || "road_disruption";

let client, coll;

async function init() {
  if (!MONGODB_URI || !/^mongodb(\+srv)?:\/\//.test(MONGODB_URI)) {
    console.error("❌ Missing/invalid MONGODB_URI. Put it in api/.env");
    process.exit(1);
  }
  client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db(DB_NAME);
  coll = db.collection(COLL);
  console.log(`✅ Connected to MongoDB (db=${DB_NAME}, coll=${COLL})`);
}
init().catch((e) => {
  console.error("Mongo init error:", e);
  process.exit(1);
});

// Simple health check
app.get("/api/health", (req, res) => {
  res.json({ ok: true, db: DB_NAME, coll: COLL });
});

// Normalize one doc -> frontend shape
function normalize(doc) {
  const lat = doc.lat ?? doc.latitude;
  const lon = doc.lon ?? doc.lng ?? doc.longitude;
  return {
    lat: typeof lat === "string" ? parseFloat(lat) : lat,
    lon: typeof lon === "string" ? parseFloat(lon) : lon,
    severity: doc.severity,
    provider_ts: doc.provider_ts,
    timestamp: doc.timestamp,
    road: doc.road ?? doc.road_name,
  };
}

// GET /api/disruptions?severity=Serious&since=2025-09-01&limit=20000
app.get("/api/disruptions", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || "20000", 10), 50000);
  const { severity, since } = req.query;

  try {
    const match = {};
    if (severity) match.severity = severity;

    if (since) {
      const dt = new Date(since);
      if (!isNaN(dt.getTime())) {
        const iso = dt.toISOString();
        match.$or = [
          { provider_ts: { $gte: iso } },
          { timestamp: { $gte: iso } },
        ];
      } else {
        console.warn("⚠️ Ignoring invalid 'since' date:", since);
      }
    }

    const pipeline = [
      { $match: match },
      {
        $project: {
          _id: 0,
          lat: { $ifNull: ["$lat", "$latitude"] },
          lon: { $ifNull: ["$lon", "$lng", "$longitude"] },
          severity: 1,
          provider_ts: 1,
          timestamp: 1,
          road: "$road_name",
          road_name: 1,
        },
      },
      { $match: { lat: { $ne: null }, lon: { $ne: null } } },
      { $limit: limit },
    ];

    console.log("ℹ️ Running aggregation:", JSON.stringify(pipeline));

    let data = await coll.aggregate(pipeline /* .allowDiskUse(true) */).toArray();

    // Fallback if aggregation returns empty suspiciously
    if (!data || data.length === 0) {
      console.warn("⚠️ Aggregation returned 0 docs — trying fallback find() with projection...");
      const cursor = coll.find(
        { $or: [{ lat: { $ne: null } }, { latitude: { $ne: null } }], $or_2: [{ lon: { $ne: null } }, { lng: { $ne: null } }, { longitude: { $ne: null } }] },
        { projection: { _id: 0, lat: 1, lon: 1, latitude: 1, lng: 1, longitude: 1, severity: 1, provider_ts: 1, timestamp: 1, road_name: 1 } }
      ).limit(limit);

      data = (await cursor.toArray()).filter(d => {
        const la = d.lat ?? d.latitude;
        const lo = d.lon ?? d.lng ?? d.longitude;
        return la != null && lo != null;
      });
    }

    // Normalize & filter invalid numbers
    const out = [];
    let dropped = 0;
    for (const d of data) {
      const n = normalize(d);
      if (isFinite(n.lat) && isFinite(n.lon)) out.push(n);
      else dropped++;
    }
    if (dropped) console.warn(`⚠️ Dropped ${dropped} docs with non-numeric lat/lon`);

    res.json(out);
  } catch (e) {
    // Log full error on server and send minimal info to client
    console.error("❌ /api/disruptions error:", e?.stack || e);
    res.status(500).json({ error: "Failed to fetch disruptions" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API up on http://localhost:${PORT}`));
