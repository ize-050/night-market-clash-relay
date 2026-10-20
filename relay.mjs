import { createServer } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

// One process owns each room. Deploy one instance; scaling requires shared routing.
export function createRelay({ origins = [], maxRooms = 1000 } = {}) {
  const rooms = new Map();
  const server = createServer((req, res) => {
    res.writeHead(req.url === '/health' ? 200 : 404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: req.url === '/health' }));
  });
  const wss = new WebSocketServer({ server, maxPayload: 65536, perMessageDeflate: false });
  const send = (ws, value) => { if (ws?.readyState === WebSocket.OPEN && ws.bufferedAmount < 262144) ws.send(JSON.stringify(value)); };
  wss.on('connection', (ws, req) => {
    if (origins.length && !origins.includes(req.headers.origin)) return ws.close(1008, 'Origin denied');
    let room, slot, count = 0, windowAt = Date.now();
    ws.alive = true;
    ws.on('pong', () => { ws.alive = true; });
    const admission = setTimeout(() => ws.close(1008, 'Join timeout'), 10000);
    const reject = reason => { send(ws, { type: 'relay-error', reason }); ws.close(1008, 'Cannot join'); };
    ws.on('message', bytes => {
      if (Date.now() - windowAt > 1000) { count = 0; windowAt = Date.now(); }
      if (++count > 150) return ws.close(1008, 'Rate limit');
      let m; try { m = JSON.parse(bytes.toString()); } catch { return ws.close(1008, 'Invalid JSON'); }
      if (!m || typeof m !== 'object') return;
      if (!room) {
        if (m.type !== 'join' || !/^[A-Z2-9]{8}$/.test(m.code) || typeof m.token !== 'string' || m.token.length > 80 || typeof m.version !== 'string' || typeof m.host !== 'boolean') return reject('ข้อมูลห้องไม่ถูกต้อง');
        let found = rooms.get(m.code);
        if (!found) {
          if (!m.host) return reject('ไม่พบห้อง ให้เจ้าของห้องเปิดเกมและส่งรหัสใหม่');
          if (rooms.size >= maxRooms) return reject('เซิร์ฟเวอร์เต็ม กรุณาลองใหม่');
          found = { code: m.code, version: m.version, slots: [null, null], tokens: [m.token, ''], expires: 0 };
          rooms.set(m.code, found);
        }
        slot = m.host ? 0 : 1;
        if (found.version !== m.version) return reject('เวอร์ชันเกมต่างกัน รีเฟรชทั้งสองเครื่อง');
        if (found.slots[slot] || (found.tokens[slot] && found.tokens[slot] !== m.token)) return reject('ห้องเต็ม หรือรหัสห้องซ้ำ');
        room = found; room.slots[slot] = ws; room.tokens[slot] = m.token; room.expires = 0;
        clearTimeout(admission); send(ws, { type: 'relay-ready' });
        if (room.slots.every(Boolean)) room.slots.forEach(client => send(client, { type: 'relay-connected' }));
        return;
      }
      if (m.type === 'packet' && m.payload && typeof m.payload.type === 'string') send(room.slots[1 - slot], m);
    });
    ws.on('error', () => {});
    ws.on('close', () => {
      clearTimeout(admission);
      if (!room || room.slots[slot] !== ws) return;
      room.slots[slot] = null; room.expires = Date.now() + 60000;
      send(room.slots[1 - slot], { type: 'relay-disconnected' });
    });
  });
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) { if (!ws.alive) ws.terminate(); else { ws.alive = false; ws.ping(); } }
    for (const [code, room] of rooms) if (room.expires && room.expires < Date.now()) {
      room.slots.forEach(ws => ws?.close(1000, 'Room expired')); rooms.delete(code);
    }
  }, 15000);
  server.on('close', () => { clearInterval(heartbeat); wss.close(); });
  return server;
}
if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const origins = (process.env.ALLOWED_ORIGINS || 'https://night-market-clash.vercel.app').split(',');
  createRelay({ origins }).listen(Number(process.env.PORT) || 8080, '0.0.0.0');
}
