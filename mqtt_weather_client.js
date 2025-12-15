
const BROKER_URL = process.env.BROKER_URL || "mqtt://localhost:1883"; // im Container: mqtt://mosquitto:1883
const TOPIC = process.env.TOPIC || "weather";
const CLIENT_ID = process.env.CLIENT_ID || "weather_dashboard_client_js";
const OUTAGE_SECONDS = Number(process.env.OUTAGE_SECONDS || 30);
const REFRESH_MS = Number(process.env.REFRESH_MS || 1000);
const HIDE_INVALID = (process.env.HIDE_INVALID || "0") === "1";

const VALID_TEMP_RANGE = { min: -50.0, max: 80.0 };
const VALID_HUM_RANGE = { min: 0.0, max: 100.0 };

const mqtt = require("mqtt");
const fs = require("fs");

function nowEpoch() { return Date.now() / 1000; }
function toLocal(dt) {
  const d = new Date(dt * 1000);
  const pad = (n)=> String(n).padStart(2,"0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function dateKeyFromEpoch(sec){ const d=new Date(sec*1000); return d.toISOString().slice(0,10); } // YYYY-MM-DD
function hourBucketFromEpoch(sec){ const d=new Date(sec*1000); return d.toISOString().slice(0,13); } // YYYY-MM-DDTHH

class StationState {
  constructor(id){
    this.id = id;
    this.lastSeen = null;         
    this.lastPayload = null;      
    this.lastValid = false;
    this.window = [];             
    this.dayMin = null;           
    this.dayMax = null;          
    this.hourly = {};             
    this.outageNextLogTs = 0;
  }
  pushValid(ts, t, h){
    const cutoff = ts - 300;
    this.window.push({ts,t,h});
    while(this.window.length && this.window[0].ts < cutoff){
      this.window.shift();
    }
    const d = dateKeyFromEpoch(ts);
    if(!this.dayMin || this.dayMin.date !== d){
      this.dayMin = {date:d, t, h};
      this.dayMax = {date:d, t, h};
    }else{
      if(t < this.dayMin.t) this.dayMin.t = t;
      if(h < this.dayMin.h) this.dayMin.h = h;
      if(t > this.dayMax.t) this.dayMax.t = t;
      if(h > this.dayMax.h) this.dayMax.h = h;
    }
    const b = hourBucketFromEpoch(ts);
    const acc = this.hourly[b] || {count:0,sumT:0,sumH:0,minT:t,maxT:t,minH:h,maxH:h};
    acc.count += 1; acc.sumT += t; acc.sumH += h;
    if(t < acc.minT) acc.minT = t; if(t > acc.maxT) acc.maxT = t;
    if(h < acc.minH) acc.minH = h; if(h > acc.maxH) acc.maxH = h;
    this.hourly[b] = acc;
  }
  fiveMinAvg(){
    if(this.window.length === 0) return {t:null,h:null};
    let st=0, sh=0;
    for(const w of this.window){ st += w.t; sh += w.h; }
    return { t: st/this.window.length, h: sh/this.window.length };
  }
  popCompletedHours(nowBucket){
    const done = [];
    for(const b of Object.keys(this.hourly).sort()){
      if(b < nowBucket){
        done.push([b, this.hourly[b]]);
        delete this.hourly[b];
      }
    }
    return done;
  }
}

const stations = new Map();
const outageLogPath = "outages.log";

function parseMessage(buf){
  try {
    const obj = JSON.parse(buf.toString("utf8"));
    if (obj && typeof obj === "object") return obj;
  } catch(_){}
  return null;
}
function extractFields(obj){
  const stationId = obj.station_id || obj.stationId || null;
  const temperature = obj.temperature;
  const humidity = obj.humidity;
  let ts = null;
  if(obj.timestamp){
    const s = String(obj.timestamp).replace("Z","");
    const d = new Date(s);
    if(!isNaN(d.getTime())) ts = d.getTime()/1000;
  }
  return {stationId, temperature, humidity, ts};
}
function isValid(temp, hum){
  if (temp === undefined || hum === undefined || temp === null || hum === null) return false;
  const t = Number(temp), h = Number(hum);
  if (!isFinite(t) || !isFinite(h)) return false;
  if (t === -999) return false;
  if (t < VALID_TEMP_RANGE.min || t > VALID_TEMP_RANGE.max) return false;
  if (h < VALID_HUM_RANGE.min || h > VALID_HUM_RANGE.max) return false;
  return true;
}

function fmtNum(v){ return (v===null || v===undefined) ? "-" : (typeof v === "number" ? v.toFixed(1) : String(v)); }
function clear() { process.stdout.write("\x1b[2J\x1b[H"); }

function render(){
  clear();
  process.stdout.write(`MQTT Weather Client  |  Broker: ${BROKER_URL}  |  Topic: ${TOPIC}\n`);
  process.stdout.write("-".repeat(100) + "\n");
  process.stdout.write(
    `${"Station".padEnd(12)} ${"Temp".padStart(8)} ${"Hum".padStart(8)} ${"Valid".padStart(7)} ${"Last Seen".padStart(20)} ${"5m Avg T/H".padStart(20)}\n`
  );
  process.stdout.write("-".repeat(100) + "\n");

  const now = nowEpoch();
  const keys = [...stations.keys()].sort();
  for(const sid of keys){
    const st = stations.get(sid);
    const lp = st.lastPayload || {};
    let t = (typeof lp.temperature === "number") ? lp.temperature : Number(lp.temperature);
    let h = (typeof lp.humidity === "number") ? lp.humidity : Number(lp.humidity);
    if (!isFinite(t)) t = null;
    if (!isFinite(h)) h = null;
    const avg = st.fiveMinAvg();
    const lastSeenStr = st.lastSeen ? toLocal(st.lastSeen) : "-";
    const validMark = st.lastValid ? "OK" : "⚠︎";

    let tDisp = t, hDisp = h;
    if(!st.lastValid && HIDE_INVALID){ tDisp = null; hDisp = null; }

    process.stdout.write(
      `${sid.padEnd(12)} ${fmtNum(tDisp).padStart(8)} ${fmtNum(hDisp).padStart(8)} ${validMark.padStart(7)} ${lastSeenStr.padStart(20)} ${(fmtNum(avg.t)+"/"+fmtNum(avg.h)).padStart(20)}\n`
    );
  }

  for(const sid of keys){
    const st = stations.get(sid);
    if(st.dayMin && st.dayMax){
      process.stdout.write(
        `Day Min/Max ${sid.padEnd(8)}  T: ${st.dayMin.t.toFixed(1)}/${st.dayMax.t.toFixed(1)}  H: ${st.dayMin.h.toFixed(1)}/${st.dayMax.h.toFixed(1)}\n`
      );
    }
  }

  const nowBucket = hourBucketFromEpoch(now);
  for(const sid of keys){
    const st = stations.get(sid);
    const completed = st.popCompletedHours(nowBucket);
    for(const [bucket, acc] of completed){
      const avgT = acc.sumT/acc.count;
      const avgH = acc.sumH/acc.count;
      process.stdout.write(
        `[Hourly] ${sid} ${bucket}: count=${acc.count} ` +
        `T(avg/min/max)=${avgT.toFixed(1)}/${acc.minT.toFixed(1)}/${acc.maxT.toFixed(1)} ` +
        `H(avg/min/max)=${avgH.toFixed(1)}/${acc.minH.toFixed(1)}/${acc.maxH.toFixed(1)}\n`
      );
    }
  }

  for(const sid of keys){
    const st = stations.get(sid);
    if(!st.lastSeen) continue;
    const silent = now - st.lastSeen;
    if (silent > OUTAGE_SECONDS){
      if (now >= st.outageNextLogTs){
        const line = `[ALERT] ${toLocal(now)} Station ${sid} OUTAGE (${Math.floor(silent)}s no data)`;
        process.stdout.write(line + "\n");
        try { fs.appendFileSync(outageLogPath, line + "\n", "utf8"); } catch(_){}
        st.outageNextLogTs = now + 10; 
      }
    }
  }
}

const client = mqtt.connect(BROKER_URL, {
  clientId: CLIENT_ID,
  reconnectPeriod: 2000,
  keepalive: 60
});

client.on("connect", () => {
  console.log(`[MQTT] Connected: ${BROKER_URL}`);
  client.subscribe(TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("[MQTT] Subscribe error:", err);
  });
});

client.on("reconnect", () => {
  console.log("[MQTT] Reconnecting...");
});

client.on("close", () => {
  console.log("[MQTT] Connection closed.");
});

client.on("error", (err) => {
  console.error("[MQTT] Error:", err && err.message ? err.message : err);
});

client.on("message", (_topic, payload) => {
  const now = nowEpoch();
  const obj = parseMessage(payload);
  if(!obj) return;

  const {stationId, temperature, humidity, ts} = extractFields(obj);
  if(!stationId) return;

  let st = stations.get(stationId);
  if(!st){ st = new StationState(stationId); stations.set(stationId, st); }

  st.lastSeen = now;
  st.lastPayload = obj;

  const valid = isValid(temperature, humidity);
  st.lastValid = valid;

  const tsStats = ts || now;
  if(valid){
    st.pushValid(tsStats, Number(temperature), Number(humidity));
  }
});

setInterval(render, REFRESH_MS);

process.on("SIGINT", ()=>{ console.log("\nStopping..."); client.end(true, ()=>process.exit(0));});
process.on("SIGTERM", ()=>{ console.log("\nStopping..."); client.end(true, ()=>process.exit(0));});
