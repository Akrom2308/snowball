const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(INDEX, (err, html) => {
    if (err) { res.writeHead(500); return res.end('index.html topilmadi'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(html);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
const rooms = new Map(); // kod -> { host, guest }
const CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += CH[Math.floor(Math.random() * CH.length)];
    if (!rooms.has(c)) return c;
  }
}
function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function leave(ws) {
  if (!ws.room) return;
  const r = rooms.get(ws.room);
  if (r) {
    if (ws.role === 'host') {
      send(r.guest, { t: 'peer-left' });
      rooms.delete(ws.room);
    } else if (r.guest === ws) {
      r.guest = null;
      send(r.host, { t: 'peer-left' });
    }
  }
  ws.room = null; ws.role = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null; ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    ws.isAlive = true;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'host') {
      leave(ws);
      if (rooms.size > 2000) return send(ws, { t: 'error', e: 'busy' });
      const code = newCode();
      rooms.set(code, { host: ws, guest: null });
      ws.room = code; ws.role = 'host';
      send(ws, { t: 'hosted', code });
    } else if (m.t === 'join') {
      leave(ws);
      const code = String(m.code || '').toUpperCase().slice(0, 4);
      const r = rooms.get(code);
      if (!r) return send(ws, { t: 'error', e: 'nofound' });
      if (r.guest) return send(ws, { t: 'error', e: 'full' });
      r.guest = ws; ws.room = code; ws.role = 'guest';
      send(ws, { t: 'joined' });
      send(r.host, { t: 'peer-joined' });
    } else if (m.t === 'signal') {
      const r = ws.room && rooms.get(ws.room);
      if (!r) return;
      send(ws.role === 'host' ? r.guest : r.host, { t: 'signal', data: m.data });
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, 30000);

server.listen(PORT, () => console.log('Qor jangi serveri ishga tushdi: ' + PORT));
