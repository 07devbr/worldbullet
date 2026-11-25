// =====================================================================
//  Servidor do WorldBullet (multiplayer) — Node.js puro, sem instalar nada
//  - Entrega o jogo (index.html) em  http://SEU-SITE/
//  - Liga os jogadores entre si em   ws://SEU-SITE/ws
//  Cada jogador manda a sua "presença" (posição, tiros, estado da partida...)
//  e o servidor repassa para quem está na mesma sala (mesmo código).
//  Quem não está na sala recebe só o resumo (nome e código) para a lista de salas.
// =====================================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');
const MAX_MSG = 16 * 1024; // tamanho máximo de uma mensagem (presença tem ~4 KB)
const MAX_RATE = 40; // mensagens por segundo por jogador (o jogo manda 10)
const MAX_PLAYERS = 300; // jogadores ao mesmo tempo no servidor

// ---------------- site: entrega o jogo ----------------
let gameHtml = null,
  gameGz = null,
  gameTag = '';
function loadGame() {
  try {
    gameHtml = fs.readFileSync(GAME);
  } catch (e) {
    gameHtml = Buffer.from('<h1>Coloque o index.html do jogo na mesma pasta do server.js</h1>');
  }
  gameGz = zlib.gzipSync(gameHtml, { level: 9 }); // o jogo vai compactado (metade do tamanho)
  gameTag = '"' + crypto.createHash('md5').update(gameHtml).digest('hex') + '"';
}
loadGame();
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    // quem já baixou e o jogo não mudou: não baixa de novo
    if (req.headers['if-none-match'] === gameTag) {
      res.writeHead(304, { ETag: gameTag });
      return res.end();
    }
    const gz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', ETag: gameTag, Vary: 'Accept-Encoding', ...(gz ? { 'Content-Encoding': 'gzip' } : {}) });
    return res.end(gz ? gameGz : gameHtml);
  }
  if (url === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, jogadores: clients.size, salas: roomCount(), enviadoMB: +(sentBytes / 1e6).toFixed(1) }));
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Não encontrado');
});

// ---------------- WebSocket (feito à mão, sem biblioteca) ----------------
const clients = new Map(); // id -> cliente
let nextId = 1;
const okCode = (s) => typeof s === 'string' && /^[A-Z0-9]{3,8}$/.test(s);
function roomCount() {
  const S = new Set();
  for (const c of clients.values()) if (c.code) S.add(c.code);
  return S.size;
}

function frame(data, comp) {
  const n = data.length,
    b0 = comp ? 0xc1 : 0x81; // 0x40 = mensagem compactada
  let head;
  if (n < 126) head = Buffer.from([b0, n]);
  else if (n < 65536) {
    head = Buffer.alloc(4);
    head[0] = b0;
    head[1] = 126;
    head.writeUInt16BE(n, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = b0;
    head[1] = 127;
    head.writeBigUInt64BE(BigInt(n), 2);
  }
  return Buffer.concat([head, data]);
}
// mensagem pronta para mandar: normal e compactada (feita uma vez só, mesmo indo para vários jogadores)
const TAIL = Buffer.from([0x00, 0x00, 0xff, 0xff]);
function pack(obj) {
  const raw = Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj));
  let plain = null,
    def = null;
  return {
    get(c) {
      if (c.deflate && raw.length > 100) {
        if (!def) {
          let z = zlib.deflateRawSync(raw, { finishFlush: zlib.constants.Z_SYNC_FLUSH });
          if (z.length >= 4 && z.slice(-4).equals(TAIL)) z = z.slice(0, -4);
          def = frame(z, true);
        }
        return def;
      }
      return plain || (plain = frame(raw, false));
    },
  };
}
let sentBytes = 0;
function send(c, msg) {
  if (!c.alive) return;
  try {
    // se o celular do jogador estiver muito atrasado (internet ruim), pula mensagens velhas
    if (c.sock.writableLength > 512 * 1024) return;
    const b = (msg && msg.get ? msg : pack(msg)).get(c);
    sentBytes += b.length;
    c.sock.write(b);
  } catch (e) {}
}
function closeClient(c) {
  if (!c.alive) return;
  c.alive = false;
  clients.delete(c.id);
  try {
    c.sock.destroy();
  } catch (e) {}
  const msg = pack({ t: 'x', id: c.id });
  for (const o of clients.values()) send(o, msg);
  broadcastCount();
}
let countT = null;
function broadcastCount() {
  if (countT) return;
  countT = setTimeout(() => {
    countT = null;
    const msg = pack({ t: 'n', n: clients.size });
    for (const o of clients.values()) send(o, msg);
  }, 500);
}
// resumo para quem está fora da sala (lista de salas abertas)
const light = (pr) => (pr ? { v: pr.v, n: pr.n, s: pr.s, u: pr.u, st: pr.st === 'play' || pr.st === 'dead' ? pr.st : null, q: pr.q || null, qt: pr.qt || null, qk: pr.qk || null } : null);
const lightKey = (pr) => (pr ? [pr.n, pr.s, pr.u, pr.q, pr.qt, pr.qk, pr.st === 'play' || pr.st === 'dead'].join('|') : '');

function onPresence(c, pr) {
  if (!pr || typeof pr !== 'object') return;
  const oldCode = c.code,
    oldLight = lightKey(c.pr);
  c.pr = pr;
  c.code = okCode(pr.s) ? pr.s : null;
  const full = pack({ t: 'p', id: c.id, pr }),
    lightChanged = lightKey(pr) !== oldLight,
    lmsg = lightChanged ? pack({ t: 'p', id: c.id, pr: light(pr) }) : null;
  for (const o of clients.values()) {
    if (o === c) continue;
    if (c.code && o.code === c.code) send(o, full);
    else if (lmsg) send(o, lmsg);
  }
  // entrou numa sala nova: recebe o estado completo de quem já está nela
  if (c.code && c.code !== oldCode) {
    for (const o of clients.values()) if (o !== c && o.code === c.code && o.pr) send(c, { t: 'p', id: o.id, pr: o.pr });
  }
  // saiu da sala: quem ficou lá recebe o resumo dele (o jogo tira ele da partida)
  if (oldCode && oldCode !== c.code && !lightChanged) {
    const m = pack({ t: 'p', id: c.id, pr: light(pr) });
    for (const o of clients.values()) if (o !== c && o.code === oldCode) send(o, m);
  }
}
function onMessage(c, text) {
  const now = Date.now();
  if (now - c.rateT > 1000) {
    c.rateT = now;
    c.rateN = 0;
  }
  if (++c.rateN > MAX_RATE) return;
  let m;
  try {
    m = JSON.parse(text);
  } catch (e) {
    return;
  }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'p') onPresence(c, m.pr);
  else if (m.t === 'e' && typeof m.ev === 'string' && m.ev.length < 32) {
    const msg = pack({ t: 'e', id: c.id, ev: m.ev, d: m.d });
    for (const o of clients.values()) if (o !== c && (!c.code || o.code === c.code)) send(o, msg);
  } else if (m.t === 'ping') send(c, { t: 'pong' });
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if ((req.url || '').split('?')[0] !== '/ws' || !key) return sock.destroy();
  if (clients.size >= MAX_PLAYERS) {
    sock.end('HTTP/1.1 503 Service Unavailable\r\n\r\n');
    return;
  }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  // compactação (permessage-deflate): mensagens de ~1 KB viram ~250 bytes
  const deflate = /permessage-deflate/.test(req.headers['sec-websocket-extensions'] || '');
  sock.write(
    'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
      accept +
      (deflate ? '\r\nSec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover' : '') +
      '\r\n\r\n',
  );
  sock.setNoDelay(true);
  const c = { id: 'j' + nextId++, sock, alive: true, deflate, pr: null, code: null, buf: Buffer.alloc(0), frag: [], fragZ: false, rateT: 0, rateN: 0, seen: Date.now() };
  clients.set(c.id, c);
  // boas-vindas: seu número e o resumo de quem já está no servidor
  send(c, { t: 'hi', id: c.id, n: clients.size });
  for (const o of clients.values()) if (o !== c && o.pr) send(c, { t: 'p', id: o.id, pr: light(o.pr) });
  broadcastCount();

  sock.on('data', (chunk) => {
    c.seen = Date.now();
    c.buf = Buffer.concat([c.buf, chunk]);
    while (c.buf.length >= 2) {
      const b0 = c.buf[0],
        b1 = c.buf[1],
        fin = (b0 & 0x80) !== 0,
        op = b0 & 0x0f,
        masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f,
        off = 2;
      if (len === 126) {
        if (c.buf.length < 4) return;
        len = c.buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (c.buf.length < 10) return;
        const big = c.buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_MSG)) return closeClient(c);
        len = Number(big);
        off = 10;
      }
      if (len > MAX_MSG || !masked) return closeClient(c); // navegador sempre manda mascarado
      if (c.buf.length < off + 4 + len) return;
      const mask = c.buf.slice(off, off + 4),
        data = Buffer.from(c.buf.slice(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      c.buf = c.buf.slice(off + 4 + len);
      if (op === 0x8) return closeClient(c); // fechou
      if (op === 0x9) {
        // ping -> pong
        try {
          const h = Buffer.from([0x8a, data.length]);
          sock.write(Buffer.concat([h, data]));
        } catch (e) {}
        continue;
      }
      if (op === 0xa) continue; // pong
      if (op === 0x1 || op === 0x0) {
        if (op === 0x1) c.fragZ = (b0 & 0x40) !== 0; // primeiro pedaço diz se veio compactada
        c.frag.push(data);
        if (c.frag.reduce((a, b) => a + b.length, 0) > MAX_MSG) return closeClient(c);
        if (fin) {
          let body = Buffer.concat(c.frag);
          c.frag = [];
          if (c.fragZ) {
            try {
              body = zlib.inflateRawSync(Buffer.concat([body, TAIL]), { maxOutputLength: MAX_MSG * 4, finishFlush: zlib.constants.Z_SYNC_FLUSH });
            } catch (e) {
              return closeClient(c);
            }
          }
          onMessage(c, body.toString('utf8'));
        }
      }
    }
  });
  sock.on('close', () => closeClient(c));
  sock.on('error', () => closeClient(c));
});

// derruba quem sumiu sem avisar (celular sem sinal)
setInterval(() => {
  const now = Date.now();
  for (const c of clients.values()) if (now - c.seen > 30000) closeClient(c);
}, 10000);

server.listen(PORT, () => console.log('WorldBullet no ar na porta ' + PORT));
