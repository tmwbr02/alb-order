#!/usr/bin/env node
/* 摊车订单 · 本地主机（电脑或树莓派用）
   用法：node server.js [埠号，预设 8080]
   同一个 WiFi 的手机用浏览器打开画面上显示的网址即可，不需要外网，也不需要安装任何套件。 */
"use strict";
var http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto"), os = require("os"), dgram = require("dgram");
var PORT = Number(process.argv[2]) || 8080;
var DIR = __dirname;
var DATA_FILE = path.join(DIR, "xlb-data.json");
var PAGE_FILE = path.join(DIR, "index.html");

var data = {};
try { data = JSON.parse(fs.readFileSync(DATA_FILE, "utf8")) || {}; } catch (e) { data = {}; }
var saveT = null;
function saveSoon() {
  clearTimeout(saveT);
  saveT = setTimeout(function () {
    var tmp = DATA_FILE + ".tmp";
    fs.writeFile(tmp, JSON.stringify(data), function (err) { if (!err) fs.rename(tmp, DATA_FILE, function () {}); });
  }, 300);
}
function clone(v) { return v === undefined ? null : JSON.parse(JSON.stringify(v)); }
function wOk(d, w) {
  for (var i = 0; i < (w || []).length; i++) {
    var c = w[i], v = d[c[0]], x = c[2];
    var ok = c[1] === "==" ? v === x : c[1] === "<" ? v < x : c[1] === "<=" ? v <= x : c[1] === ">" ? v > x : c[1] === ">=" ? v >= x : c[1] === "!=" ? v !== x : true;
    if (!ok) return false;
  }
  return true;
}
function query(col, w, lim) {
  var pre = col + "/", out = [];
  Object.keys(data).sort().forEach(function (k) {
    if (k.indexOf(pre) !== 0 || k.indexOf("/", pre.length) >= 0) return;
    if (wOk(data[k], w)) out.push({ id: k.slice(pre.length), data: data[k] });
  });
  return lim ? out.slice(0, lim) : out;
}
function payload(spec) {
  if (spec.k === "d") { var d = data[spec.p]; return { exists: !!d, data: d || null }; }
  return { docs: query(spec.p, spec.w, spec.l) };
}
var subs = [], locks = {};
function poke(s) {
  var j = JSON.stringify(payload(s.spec));
  if (j === s.last) return;
  s.last = j;
  s.conn.send('{"t":"snap","s":' + JSON.stringify(s.sid) + ',"p":' + j + "}");
}
function changed() { subs.slice().forEach(poke); saveSoon(); }
function exec(op, a) {
  a = a || [];
  if (op === "get") { var d = data[a[0]]; return { exists: !!d, data: d ? clone(d) : null }; }
  if (op === "set") { data[a[0]] = clone(a[1]); changed(); return null; }
  if (op === "update") {
    if (!data[a[0]]) throw new Error("not-found");
    var n = clone(a[1]) || {}; Object.keys(n).forEach(function (k) { data[a[0]][k] = n[k]; });
    changed(); return null;
  }
  if (op === "del") { delete data[a[0]]; changed(); return null; }
  if (op === "query") return { docs: clone(query(a[0], a[1], a[2])) };
  if (op === "acquire") {
    var o = a[1] || {}, now = Date.now(), L = locks[a[0]];
    if (L && L.h !== o.holder && L.until > now) return { acquired: false };
    locks[a[0]] = { h: o.holder, until: now + (o.ttlMs || 1000) };
    return { acquired: true };
  }
  throw new Error("bad-op");
}
function onMessage(conn, txt) {
  var m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m) return;
  if (m.t === "ping") { conn.send('{"t":"pong"}'); return; }
  if (m.t === "sub") {
    conn.subs[m.s] && removeSub(conn.subs[m.s]);
    var s = { conn: conn, sid: m.s, spec: m.spec || {}, last: null };
    conn.subs[m.s] = s; subs.push(s); poke(s); return;
  }
  if (m.t === "unsub") { if (conn.subs[m.s]) { removeSub(conn.subs[m.s]); delete conn.subs[m.s]; } return; }
  if (m.t === "req") {
    var res;
    try { res = { t: "res", i: m.i, ok: true, r: exec(m.op, m.a) }; }
    catch (e) { res = { t: "res", i: m.i, ok: false, e: String(e.message || e) }; }
    conn.send(JSON.stringify(res));
  }
}
function removeSub(s) { var i = subs.indexOf(s); if (i >= 0) subs.splice(i, 1); }

/* ---- 极简 WebSocket ---- */
function wsFrame(op, buf) {
  var len = buf.length, head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(Math.floor(len / 4294967296), 2); head.writeUInt32BE(len >>> 0, 6); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, buf]);
}
var conns = [];
function attach(sock) {
  var conn = { subs: {}, alive: true, buf: Buffer.alloc(0), frag: [] };
  conn.send = function (s) { if (conn.alive) { try { sock.write(wsFrame(1, Buffer.from(s, "utf8"))); } catch (e) {} } };
  conn.close = function () {
    if (!conn.alive) return;
    conn.alive = false;
    Object.keys(conn.subs).forEach(function (k) { removeSub(conn.subs[k]); });
    conns.splice(conns.indexOf(conn), 1);
    try { sock.destroy(); } catch (e) {}
    peersChanged();
    log("设备离线，目前 " + conns.length + " 台");
  };
  conns.push(conn);
  peersChanged();
  log("设备连上，目前 " + conns.length + " 台");
  sock.setNoDelay(true);
  sock.setKeepAlive(true, 10000);
  sock.on("data", function (chunk) {
    conn.buf = Buffer.concat([conn.buf, chunk]);
    for (;;) {
      var b = conn.buf; if (b.length < 2) return;
      var fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80, len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); off = 10; }
      var mo = off; if (masked) off += 4;
      if (b.length < off + len) return;
      var pl = Buffer.from(b.slice(off, off + len));
      if (masked) for (var i = 0; i < pl.length; i++) pl[i] ^= b[mo + (i & 3)];
      conn.buf = b.slice(off + len);
      if (op === 8) { try { sock.write(wsFrame(8, Buffer.alloc(0))); } catch (e) {} conn.close(); return; }
      if (op === 9) { try { sock.write(wsFrame(10, pl)); } catch (e) {} continue; }
      if (op === 10) continue;
      if (op === 1 || op === 2 || op === 0) {
        conn.frag.push(pl);
        if (fin) { var txt = Buffer.concat(conn.frag).toString("utf8"); conn.frag = []; onMessage(conn, txt); }
      }
    }
  });
  sock.on("close", conn.close);
  sock.on("error", conn.close);
}
function peersChanged() { var m = '{"t":"peers","n":' + conns.length + '}'; conns.forEach(function (c) { c.send(m); }); }
setInterval(function () { conns.forEach(function (c) { c.send('{"t":"pong"}'); }); }, 15000);

function page() {
  var html;
  try { html = fs.readFileSync(PAGE_FILE, "utf8"); } catch (e) { return null; }
  return html.replace("<script>", "<script>window.XLB_SERVER=1;</script><script>");
}
var server = http.createServer(function (req, res) {
  var u = req.url.split("?")[0];
  if (u === "/" || u === "/index.html") {
    var h = page();
    if (!h) { res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" }); res.end("找不到 index.html，请把它放在 server.js 同一个资料夹。"); return; }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" }); res.end(h); return;
  }
  if (u === "/info") { res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }); res.end(JSON.stringify({ app: "xlb", port: PORT })); return; }
  if (u === "/backup.json") {
    res.writeHead(200, { "Content-Type": "application/json", "Content-Disposition": "attachment; filename=order-backup.json" });
    res.end(JSON.stringify({ app: "xlb", at: Date.now(), data: data })); return;
  }
  res.writeHead(404); res.end();
});
server.on("upgrade", function (req, sock) {
  var key = req.headers["sec-websocket-key"];
  if (req.url.split("?")[0] !== "/ws" || !key) { sock.destroy(); return; }
  var acc = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n");
  attach(sock);
});
/* 让 App 能自动找到这台主机（UDP 广播） */
try {
  var udp = dgram.createSocket({ type: "udp4", reuseAddr: true });
  udp.on("message", function (msg, r) { if (String(msg) === "XLB?") udp.send("XLB!" + PORT, r.port, r.address); });
  udp.on("error", function () {});
  udp.bind(47801);
} catch (e) {}
function ips() {
  var out = [], n = os.networkInterfaces();
  Object.keys(n).forEach(function (k) { (n[k] || []).forEach(function (a) { if (a.family === "IPv4" && !a.internal) out.push(a.address); }); });
  return out;
}
function log(s) { console.log(new Date().toLocaleTimeString() + "  " + s); }
server.listen(PORT, function () {
  console.log("摊车订单主机已启动。请让同一个 WiFi 的手机用浏览器打开：");
  ips().forEach(function (ip) { console.log("   http://" + ip + ":" + PORT); });
  console.log("资料存在：" + DATA_FILE + "（按 Ctrl+C 结束）");
});
