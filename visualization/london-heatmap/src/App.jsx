import React, { useEffect, useMemo, useRef, useState } from "react";
import { MapContainer, TileLayer, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import * as L from "leaflet";
import "leaflet.heat";

const DEFAULT_WEIGHTS = { Minor: 0.3, Moderate: 0.6, Serious: 1.0, Severe: 1.0 };

function parseWhen(s) {
  const d = s ? new Date(s) : null;
  return isNaN(d?.getTime() || NaN) ? null : d;
}

function toHeatTriples(rows, weights, now = new Date(), recencyHalfLifeDays = 0) {
  const key = (lat, lon) => `${lat.toFixed(6)},${lon.toFixed(6)}`;
  const agg = new Map();
  const ln2 = Math.log(2);

  for (const r of rows) {
    const lat = Number(r.lat ?? r.latitude);
    const lon = Number(r.lon ?? r.lng ?? r.longitude);
    if (!isFinite(lat) || !isFinite(lon)) continue;

    const sev = String(r.severity ?? "");
    const base = weights[sev] ?? 0.5;

    let decay = 1;
    if (recencyHalfLifeDays > 0) {
      const when = parseWhen(r.provider_ts || r.timestamp);
      if (when) {
        const ageDays = (now.getTime() - when.getTime()) / (1000 * 60 * 60 * 24);
        decay = Math.exp(-(ln2 * ageDays) / recencyHalfLifeDays);
      }
    }

    const w = base * decay;
    const k = key(lat, lon);
    const cur = agg.get(k);
    if (cur) cur.w += w;
    else agg.set(k, { lat, lon, w, c: 1 });
  }
  return Array.from(agg.values()).map((v) => [v.lat, v.lon, v.w]);
}

/** Ensures Leaflet knows the container size (fixes zero-size canvas) */
function MapSizer() {
  const map = useMap();
  useEffect(() => {
    // after first paint
    const t = setTimeout(() => map.invalidateSize(), 0);
    // keep in sync on window resizes
    const onResize = () => map.invalidateSize();
    window.addEventListener("resize", onResize);
    return () => {
      clearTimeout(t);
      window.removeEventListener("resize", onResize);
    };
  }, [map]);
  return null;
}

function HeatLayer({ points, radius, blur, minOpacity, maxZoom, onError }) {
  const map = useMap();
  const layerRef = useRef(null);

  useEffect(() => {
    try {
      if (!map) return;
      if (!L?.heatLayer) {
        const msg = "Leaflet heat plugin not available (L.heatLayer is undefined)";
        console.error(msg);
        onError?.(msg);
        return;
      }
      if (!layerRef.current) {
        layerRef.current = L.heatLayer(points ?? [], { radius, blur, minOpacity, maxZoom });
        layerRef.current.addTo(map);
      }
    } catch (err) {
      console.error("HeatLayer init error:", err);
      onError?.(String(err));
    }
    return () => {
      if (layerRef.current) {
        map.removeLayer(layerRef.current);
        layerRef.current = null;
      }
    };
  }, [map]);

  useEffect(() => {
    try {
      if (layerRef.current) {
        layerRef.current.setLatLngs(points ?? []);
        layerRef.current.setOptions({ radius, blur, minOpacity, maxZoom });
      }
    } catch (err) {
      console.error("HeatLayer update error:", err);
      onError?.(String(err));
    }
  }, [points, radius, blur, minOpacity, maxZoom]);

  return null;
}

export default function App() {
  const [rows, setRows] = useState([]);
  const [jsonText, setJsonText] = useState("");
  const [error, setError] = useState("");

  const [radius, setRadius] = useState(16);
  const [blur, setBlur] = useState(22);
  const [minOpacity, setMinOpacity] = useState(0.25);
  const [halfLife, setHalfLife] = useState(0);
  const [weights, setWeights] = useState({ ...DEFAULT_WEIGHTS });

  useEffect(() => {
    if (rows.length === 0) {
      const demo = [
        { lat: 51.509, lon: -0.074, severity: "Serious", provider_ts: new Date().toISOString() },
        { lat: 51.5094, lon: -0.078, severity: "Serious", provider_ts: new Date().toISOString() },
        { lat: 51.5087, lon: -0.069, severity: "Moderate", provider_ts: new Date().toISOString() },
        { lat: 51.5109, lon: -0.0398, severity: "Minor", provider_ts: new Date().toISOString() },
      ];
      setRows(demo);
      setJsonText(JSON.stringify(demo, null, 2));
    }
  }, []);

  const points = useMemo(() => toHeatTriples(rows, weights, new Date(), halfLife), [rows, weights, halfLife]);

  function handleFile(file) {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const txt = String(e.target?.result || "");
        const parsed = JSON.parse(txt);
        const arr = Array.isArray(parsed) ? parsed : parsed.data || [];
        setJsonText(JSON.stringify(arr, null, 2));
        setRows(arr);
        setError("");
        console.log("Loaded rows from file:", arr.length);
      } catch (err) {
        const msg = "Could not parse JSON: " + err.message;
        setError(msg);
        console.error(msg);
        alert(msg);
      }
    };
    reader.readAsText(file);
  }

  async function fetchFromApi() {
    try {
      const res = await fetch("/api/disruptions"); // or http://localhost:3000/api/disruptions
      if (!res.ok) throw new Error("API request failed");
      const payload = await res.json();
      const arr = Array.isArray(payload) ? payload : payload.data || [];
      setRows(arr);
      setJsonText(JSON.stringify(arr.slice(0, 50), null, 2));
      setError("");
      console.log("Fetched rows from API:", arr.length);
    } catch (e) {
      const msg = "Fetch failed: " + e.message + " — Did you start the Node server?";
      setError(msg);
      console.error(msg);
      alert(msg);
    }
  }

  function downloadCSV() {
    const triples = toHeatTriples(rows, weights);
    const csv = ["lat,lon,weight", ...triples.map(([a, b, c]) => `${a},${b},${c}`)].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "hotspots.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 2fr", gap: 16, height: "100vh", padding: 16, boxSizing: "border-box" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <h1 style={{ margin: 0 }}>London Road Disruptions — Heatmap</h1>
        <p style={{ margin: 0, color: "#555" }}>Upload JSON or fetch from your Mongo-backed API. Tweak the glow like Strava.</p>

        {error ? (
          <div style={{ background: "#fee", border: "1px solid #f99", padding: 8, borderRadius: 8, whiteSpace: "pre-wrap" }}>
            <strong>Error:</strong> {error}
          </div>
        ) : null}

        <div style={{ display: "flex", gap: 8 }}>
          <input type="file" accept=".json,application/json" onChange={(e) => e.target.files?.[0] && handleFile(e.target.files[0])} />
          <button onClick={downloadCSV}>Export hotspots</button>
          <button onClick={fetchFromApi}>Fetch from API</button>
        </div>

        <label>Radius: {radius}</label>
        <input type="range" min={4} max={60} value={radius} onChange={(e) => setRadius(Number(e.target.value))} />

        <label>Blur: {blur}</label>
        <input type="range" min={0} max={60} value={blur} onChange={(e) => setBlur(Number(e.target.value))} />

        <label>Min opacity: {minOpacity.toFixed(2)}</label>
        <input type="range" min={0} max={1} step={0.01} value={minOpacity} onChange={(e) => setMinOpacity(Number(e.target.value))} />

        <div style={{ display: "grid", gridTemplateColumns: "auto auto", gap: 8 }}>
          {Object.keys(DEFAULT_WEIGHTS).map((sev) => (
            <div key={sev} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <label style={{ width: 70 }}>{sev}</label>
              <input
                type="number"
                step={0.1}
                min={0}
                max={3}
                value={weights[sev]}
                onChange={(e) => setWeights({ ...weights, [sev]: Number(e.target.value) })}
                style={{ width: 80 }}
              />
            </div>
          ))}
        </div>

        <label>Recency half-life (days): {halfLife}</label>
        <input type="range" min={0} max={60} value={halfLife} onChange={(e) => setHalfLife(Number(e.target.value))} />

        <label>Data preview</label>
        <textarea
          style={{ width: "100%", height: 220, fontFamily: "monospace", fontSize: 12 }}
          value={jsonText}
          onChange={(e) => {
            setJsonText(e.target.value);
            try {
              const parsed = JSON.parse(e.target.value);
              setRows(Array.isArray(parsed) ? parsed : parsed.data || []);
            } catch {
              // ignore while typing
            }
          }}
        />
        <div style={{ fontSize: 12, color: "#666" }}>Rows loaded: {rows.length} | Heat points: {points.length}</div>
      </div>

      <div>
        <MapContainer
          center={[51.5074, -0.1278]}
          zoom={12}
          style={{ width: "100%", height: "100%", minHeight: 520, borderRadius: 12, boxShadow: "0 1px 6px rgba(0,0,0,0.1)" }}
        >
          <TileLayer
            attribution='&copy; OpenStreetMap contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapSizer /> {/* <-- make sure map sizes itself */}
          <HeatLayer
            points={points}
            radius={radius}
            blur={blur}
            minOpacity={minOpacity}
            maxZoom={18}
            onError={(msg) => setError(msg)}
          />
        </MapContainer>
      </div>
    </div>
  );
}
