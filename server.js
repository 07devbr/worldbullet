// =====================================================================
//  Servidor do WorldBullet (multiplayer) — Node.js puro, sem instalar nada
//  - Entrega o jogo (index.html) em  http://SEU-SITE/
//  - Liga os jogadores entre si em   ws://SEU-SITE/ws
//  Cada jogador manda a sua "presença" (posição, tiros, estado da partida...)
//  e o servidor repassa para quem está na mesma sala (mesmo código).
//  Quem não está na sala recebe só o resumo (nome e código) para a lista de salas.
//  Também guarda as CONTAS (usuário/senha, amigos, presentes) — veja a parte "CONTAS".
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
  gameTag = '',
  GAME_VER = 0; // v134: número da versão do jogo (lido do index.html)
const REIN = { ate: 0 }; // v134: aviso de atualização (o servidor vai reiniciar)
function loadGame() {
  try {
    gameHtml = fs.readFileSync(GAME);
  } catch (e) {
    gameHtml = Buffer.from('<h1>Coloque o index.html do jogo na mesma pasta do server.js</h1>');
  }
  gameGz = zlib.gzipSync(gameHtml, { level: 9 }); // o jogo vai compactado (metade do tamanho)
  gameTag = '"' + crypto.createHash('md5').update(gameHtml).digest('hex') + '"';
  const mv = /const WB_VER = (\d+);/.exec(gameHtml.toString('utf8'));
  GAME_VER = mv ? +mv[1] : 0;
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
  if (url.startsWith('/api/')) return apiHttp(req, res, url);
  if (url.startsWith('/mp/webhook')) return mpWebhook(req, res, url);
  if (url === '/ping') {
    // despertador (cron-job.org): mantém o servidor e o banco acordados
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
    if (DB.nome === 'supabase') sbReq('GET', '?select=uid&limit=1').catch(() => {});
    return res.end('ok');
  }
  if (url === '/privacidade' || url === '/privacidade/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(paginaPrivacidade());
  }
  if (url === '/status') {
    return testaBanco().then((d) => {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ ok: true, jogadores: clients.size, salas: roomCount(), contas: DB.nome, banco: d.msg, enderecoBanco: SB_URL || null, admin: ADMIN || null, email: MAIL_OK ? 'ligado (' + EMAIL_DE + ')' : 'desligado (sem BREVO_KEY/EMAIL_DE)', loja: MP_OK ? 'ligada (Mercado Pago)' : 'fechada (sem MP_TOKEN)', enviadoMB: +(sentBytes / 1e6).toFixed(1) }, null, 1));
    });
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
  if (c.uid && porConta.has(c.uid)) {
    porConta.get(c.uid).delete(c);
    if (!porConta.get(c.uid).size) porConta.delete(c.uid);
  }
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
const slOk = (a) => (Array.isArray(a) && a.length <= 6 && a.every((x) => typeof x === 'number') ? a : null);
const light = (pr) => (pr ? { v: pr.v, n: pr.n, s: pr.s, u: pr.u, vf: pr.vf || 0, cr: pr.cr ? 1 : 0, sl: slOk(pr.sl), st: pr.st === 'play' || pr.st === 'dead' ? pr.st : null, q: pr.q || null, qt: pr.qt || null, qk: pr.qk || null } : null);
const lightKey = (pr) => (pr ? [pr.n, pr.s, pr.u, pr.vf, pr.cr, JSON.stringify(slOk(pr.sl)), pr.q, pr.qt, pr.qk, pr.st === 'play' || pr.st === 'dead'].join('|') : '');

function onPresence(c, pr) {
  if (!pr || typeof pr !== 'object') return;
  // selo e ID vêm da conta conferida aqui no servidor (ninguém finge ser verificado)
  if (c.uid) {
    pr.u = c.uid;
    if (c.vf) pr.vf = c.vf;
    else delete pr.vf;
  } else delete pr.vf;
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
  else if (m.t === 'auth') wsAuth(c, typeof m.tk === 'string' ? m.tk : null);
  else if (m.t === 'e' && typeof m.ev === 'string' && m.ev.length < 32 && m.ev !== 'verif') {
    const msg = pack({ t: 'e', id: c.id, ev: m.ev, d: m.d });
    for (const o of clients.values()) if (o !== c && (!c.code || o.code === c.code)) send(o, msg);
  } else if (m.t === 'ping') send(c, { t: 'pong' });
}

// =====================================================================
//  CONTAS — usuário e senha, progresso na nuvem, amigos por ID,
//  convites, presentes e o Painel do Criador.
//  As contas ficam num banco de dados grátis (Supabase). Sem ele, ficam num
//  arquivo aqui do lado (bom para testar, mas o Render grátis apaga ao reiniciar).
// =====================================================================
// tira aspas e espaços que vêm junto quando cola no celular
const envLimpo = (k) => String(process.env[k] || '').trim().replace(/^["'`]+|["'`]+$/g, '').trim();
function arrumaUrl(u) {
  u = u.replace(/\/+$/, '').replace(/\/rest\/v1$/, '');
  // colou o link do painel (supabase.com/dashboard/project/XXXX)? vira o endereço certo
  const m = u.match(/supabase\.com\/dashboard\/project\/([a-z0-9]+)/i);
  if (m) return 'https://' + m[1] + '.supabase.co';
  if (/^[a-z0-9]{15,}$/i.test(u)) return 'https://' + u + '.supabase.co'; // colou só o código do projeto
  if (u && !/^https?:\/\//.test(u)) u = 'https://' + u;
  return u;
}
const SB_URL = arrumaUrl(envLimpo('SUPABASE_URL'));
const SB_KEY = envLimpo('SUPABASE_KEY');
const ADMIN = envLimpo('ADMIN').toLowerCase(); // seu nome de usuário: vira o Criador
const SEGREDO = process.env.SEGREDO || (SB_KEY ? crypto.createHash('sha256').update('wb-token|' + SB_KEY).digest('hex') : crypto.randomBytes(32).toString('hex'));
const DADOS = path.join(__dirname, 'dados', 'contas.json');
const PUB_COLS = 'uid,usuario,nome,vf,admin,banido,acesso,visto';
const enc = encodeURIComponent;
const agora = () => new Date().toISOString();

async function sbReq(method, query, body, prefer) {
  return sbT('contas', method, query, body, prefer);
}
async function sbT(tabela, method, query, body, prefer) {
  const h = { apikey: SB_KEY, 'Content-Type': 'application/json', Accept: 'application/json' };
  if (/^eyJ/.test(SB_KEY)) h.Authorization = 'Bearer ' + SB_KEY; // chave antiga (service_role)
  if (prefer) h.Prefer = prefer;
  const r = await fetch(SB_URL + '/rest/v1/' + tabela + query, { method, headers: h, body: body == null ? undefined : JSON.stringify(body) });
  const t = await r.text();
  if (!r.ok) {
    const e = new Error('banco ' + r.status + ' ' + t.slice(0, 300));
    e.status = r.status;
    throw e;
  }
  return t ? JSON.parse(t) : null;
}
function contaNova(o) {
  return Object.assign({ perfil: {}, stats: {}, amigos: [], pedidos: [], enviados: [], caixa: [], vf: 0, admin: false, acesso: [], banido: false, criado: agora(), visto: agora() }, o);
}
function bancoSupabase() {
  return {
    nome: 'supabase',
    async get(uid) {
      return (await sbReq('GET', '?uid=eq.' + enc(uid) + '&select=*'))[0] || null;
    },
    async byUser(u) {
      return (await sbReq('GET', '?usuario=eq.' + enc(u) + '&select=*'))[0] || null;
    },
    async many(uids) {
      if (!uids.length) return [];
      return sbReq('GET', '?uid=in.(' + uids.map(enc).join(',') + ')&select=' + PUB_COLS);
    },
    async insert(row) {
      try {
        return (await sbReq('POST', '', row, 'return=representation'))[0];
      } catch (e) {
        if (e.status === 409) return null; // usuário ou ID já existe
        throw e;
      }
    },
    async update(uid, patch) {
      return (await sbReq('PATCH', '?uid=eq.' + enc(uid), patch, 'return=representation'))[0] || null;
    },
    async remove(uid) {
      await sbReq('DELETE', '?uid=eq.' + enc(uid));
    },
    async maiorId() {
      // as contas mais novas têm os maiores IDs
      const r = await sbReq('GET', '?select=uid&order=criado.desc&limit=200');
      let m = 0;
      for (const x of r) if (ID6.test(x.uid)) m = Math.max(m, +x.uid);
      return m ? String(m) : null;
    },
    async byOld(old) {
      return (await sbReq('GET', '?perfil->>uidAntigo=eq.' + enc(old) + '&select=*'))[0] || null;
    },
    async todas() {
      return sbReq('GET', '?select=uid,usuario,criado,amigos,pedidos,enviados,perfil&order=criado.asc&limit=10000');
    },
    async onlineInfo(uids) {
      if (!uids.length) return [];
      return sbReq('GET', '?uid=in.(' + uids.map(enc).join(',') + ')&select=uid,nome,vf,banido,rp:stats->rp,st4:stats->st4,oculto:perfil->oculto');
    },
    async ranking() {
      const q = 'uid,nome,vf,banido,rp:stats->rp,st4:stats->st4,wins:stats->wins,kills:stats->kills';
      try {
        return await sbReq('GET', '?select=' + q + ',guilda&limit=5000');
      } catch (e) {
        return sbReq('GET', '?select=' + q + '&limit=5000'); // banco ainda sem a atualização v113
      }
    },
  };
}
function bancoLocal() {
  const M = new Map();
  try {
    for (const r of JSON.parse(fs.readFileSync(DADOS, 'utf8'))) M.set(r.uid, r);
  } catch (e) {}
  let t = null;
  const save = () => {
    if (t) return;
    t = setTimeout(() => {
      t = null;
      try {
        fs.mkdirSync(path.dirname(DADOS), { recursive: true });
        fs.writeFileSync(DADOS + '.tmp', JSON.stringify([...M.values()]));
        fs.renameSync(DADOS + '.tmp', DADOS);
      } catch (e) {
        console.warn('contas: não deu para salvar o arquivo', e.message);
      }
    }, 1000);
  };
  const cp = (r) => (r ? JSON.parse(JSON.stringify(r)) : null);
  const pub = (r) => {
    const o = {};
    for (const k of PUB_COLS.split(',')) o[k] = r[k];
    return cp(o);
  };
  return {
    nome: 'arquivo',
    async get(uid) {
      return cp(M.get(uid));
    },
    async byUser(u) {
      for (const r of M.values()) if (r.usuario === u) return cp(r);
      return null;
    },
    async many(uids) {
      return uids.map((u) => M.get(u)).filter(Boolean).map(pub);
    },
    async insert(row) {
      if (M.has(row.uid)) return null;
      for (const r of M.values()) if (r.usuario === row.usuario) return null;
      M.set(row.uid, contaNova(cp(row)));
      save();
      return cp(M.get(row.uid));
    },
    async update(uid, patch) {
      const r = M.get(uid);
      if (!r) return null;
      Object.assign(r, cp(patch));
      if (r.uid !== uid) {
        M.delete(uid);
        M.set(r.uid, r);
      }
      save();
      return cp(r);
    },
    async remove(uid) {
      M.delete(uid);
      save();
    },
    async maiorId() {
      let m = 0;
      for (const k of M.keys()) if (ID6.test(k)) m = Math.max(m, +k);
      return m ? String(m) : null;
    },
    async todas() {
      return [...M.values()].map(cp);
    },
    async byOld(old) {
      for (const r of M.values()) if (r.perfil && r.perfil.uidAntigo === old) return cp(r);
      return null;
    },
    async onlineInfo(uids) {
      return uids
        .map((u) => M.get(u))
        .filter(Boolean)
        .map((r) => ({ uid: r.uid, nome: r.nome, vf: r.vf, banido: r.banido, rp: (r.stats || {}).rp, st4: (r.stats || {}).st4, oculto: (r.perfil || {}).oculto }));
    },
    async ranking() {
      return [...M.values()].map((r) => ({ uid: r.uid, nome: r.nome, vf: r.vf, banido: r.banido, guilda: r.guilda || null, rp: (r.stats || {}).rp, st4: (r.stats || {}).st4, wins: (r.stats || {}).wins, kills: (r.stats || {}).kills }));
    },
  };
}
const DB = SB_URL && SB_KEY ? bancoSupabase() : bancoLocal();
// ---- guildas, denúncias e configurações (eventos) ----
function extraSupabase() {
  const um = async (t, q) => (await sbT(t, 'GET', q))[0] || null;
  return {
    g: {
      get: (id) => um('guildas', '?id=eq.' + enc(id) + '&select=*'),
      byChave: (k) => um('guildas', '?chave=eq.' + enc(k) + '&select=*'),
      list: () => sbT('guildas', 'GET', '?select=id,nome,tag,emblema,descr,membros,aberta,criado&limit=2000'),
      async insert(row) {
        try {
          return (await sbT('guildas', 'POST', '', row, 'return=representation'))[0];
        } catch (e) {
          if (e.status === 409) return null;
          throw e;
        }
      },
      update: async (id, p) => (await sbT('guildas', 'PATCH', '?id=eq.' + enc(id), p, 'return=representation'))[0] || null,
      remove: (id) => sbT('guildas', 'DELETE', '?id=eq.' + enc(id)),
    },
    d: {
      insert: async (row) => (await sbT('denuncias', 'POST', '', row, 'return=representation'))[0],
      list: (st) => sbT('denuncias', 'GET', '?status=eq.' + enc(st || 'aberta') + '&order=criado.desc&limit=100&select=*'),
      update: (id, p) => sbT('denuncias', 'PATCH', '?id=eq.' + enc(id), p),
    },
    p: {
      get: (id) => um('pedidos', '?id=eq.' + enc(id) + '&select=*'),
      insert: async (row) => (await sbT('pedidos', 'POST', '', row, 'return=representation'))[0],
      update: (id, p) => sbT('pedidos', 'PATCH', '?id=eq.' + enc(id), p),
      async marcaPago(id, mpId) {
        // só muda se ainda não estava pago (evita entregar duas vezes)
        const r = await sbT('pedidos', 'PATCH', '?id=eq.' + enc(id) + '&status=neq.pago', { status: 'pago', mp_id: mpId, pago: agora() }, 'return=representation');
        return !!(r && r.length);
      },
      lista: (uid) => sbT('pedidos', 'GET', (uid ? '?uid=eq.' + enc(uid) + '&' : '?status=eq.pago&') + 'order=criado.desc&limit=100&select=*'),
    },
    c: {
      get: async (k) => {
        const r = await um('config', '?k=eq.' + enc(k) + '&select=v');
        return r ? r.v : null;
      },
      set: (k, v) => sbT('config', 'POST', '', { k, v }, 'resolution=merge-duplicates'),
    },
  };
}
function extraLocal() {
  const ARQ = path.join(__dirname, 'dados', 'extra.json');
  let X = { guildas: {}, denuncias: [], config: {}, n: 0 };
  try {
    X = Object.assign(X, JSON.parse(fs.readFileSync(ARQ, 'utf8')));
  } catch (e) {}
  let t = null;
  const save = () => {
    if (t) return;
    t = setTimeout(() => {
      t = null;
      try {
        fs.mkdirSync(path.dirname(ARQ), { recursive: true });
        fs.writeFileSync(ARQ, JSON.stringify(X));
      } catch (e) {}
    }, 800);
  };
  const cp = (r) => (r == null ? null : JSON.parse(JSON.stringify(r)));
  X.pedidos = X.pedidos || {};
  return {
    p: {
      get: async (id) => cp(X.pedidos[id] || null),
      async insert(row) {
        X.pedidos[row.id] = Object.assign({ status: 'aberto', criado: agora() }, cp(row));
        save();
        return cp(X.pedidos[row.id]);
      },
      async update(id, p) {
        if (X.pedidos[id]) Object.assign(X.pedidos[id], cp(p));
        save();
      },
      async marcaPago(id, mpId) {
        const q = X.pedidos[id];
        if (!q || q.status === 'pago') return false;
        Object.assign(q, { status: 'pago', mp_id: mpId, pago: agora() });
        save();
        return true;
      },
      lista: async (uid) =>
        cp(
          Object.values(X.pedidos)
            .filter((q) => (uid ? q.uid === uid : q.status === 'pago'))
            .sort((a, b) => (a.criado < b.criado ? 1 : -1))
            .slice(0, 100),
        ),
    },
    g: {
      get: async (id) => cp(X.guildas[id]),
      byChave: async (k) => cp(Object.values(X.guildas).find((g) => g.chave === k) || null),
      list: async () => cp(Object.values(X.guildas)),
      async insert(row) {
        if (X.guildas[row.id] || Object.values(X.guildas).some((g) => g.chave === row.chave)) return null;
        X.guildas[row.id] = cp(row);
        save();
        return cp(row);
      },
      async update(id, p) {
        if (!X.guildas[id]) return null;
        Object.assign(X.guildas[id], cp(p));
        save();
        return cp(X.guildas[id]);
      },
      async remove(id) {
        delete X.guildas[id];
        save();
      },
    },
    d: {
      async insert(row) {
        const r = Object.assign({ id: ++X.n, status: 'aberta', criado: agora() }, cp(row));
        X.denuncias.push(r);
        X.denuncias = X.denuncias.slice(-500);
        save();
        return cp(r);
      },
      list: async (st) => cp(X.denuncias.filter((d) => d.status === (st || 'aberta')).reverse().slice(0, 100)),
      async update(id, p) {
        const d = X.denuncias.find((x) => String(x.id) === String(id));
        if (d) Object.assign(d, cp(p));
        save();
      },
    },
    c: {
      get: async (k) => cp(X.config[k] === undefined ? null : X.config[k]),
      async set(k, v) {
        X.config[k] = cp(v);
        save();
      },
    },
  };
}
const XB = DB.nome === 'supabase' ? extraSupabase() : extraLocal();
if (DB.nome === 'arquivo') console.log('⚠️  Contas num arquivo local (sem SUPABASE_URL/SUPABASE_KEY): no Render grátis elas somem quando ele reinicia.');
// diagnóstico do banco: explica em português o que está errado (aparece em /status e no Log)
let DIAG = { ok: DB.nome !== 'supabase', msg: DB.nome === 'supabase' ? 'testando...' : 'sem Supabase: contas num arquivo (somem quando o Render reinicia)' };
function explicaErro(e) {
  const t = String((e && e.message) || e);
  if (/fetch is not defined/i.test(t)) return 'Node antigo no Render. Crie a variável NODE_VERSION com o valor 20 e salve.';
  if (/Invalid API key|Invalid JWT|No API key|apikey|JWS|JWT|401/.test(t) && !/PGRST205|relation/.test(t))
    return 'A SUPABASE_KEY está errada. Copie de novo a chave Secret (sb_secret_...) em Project Settings > API Keys, sem espaço.';
  if (/PGRST205|Could not find the table|relation .* does not exist|404/.test(t)) return 'A tabela "contas" não existe. Rode o arquivo contas.sql no SQL Editor do Supabase (tem que aparecer Success).';
  if (/permission denied|42501|403/.test(t)) return 'Falta permissão na tabela. Rode o contas.sql de novo no SQL Editor (as linhas do final dão a permissão).';
  if (/ENOTFOUND|EAI_AGAIN|Invalid URL|fetch failed|ECONNREFUSED|getaddrinfo/.test(t))
    return 'A SUPABASE_URL está errada (ou o projeto está pausado). Tem que ser tipo https://abcdxyz.supabase.co — e confira no supabase.com se o projeto não está "Paused".';
  if (/PGRST|column/.test(t)) return 'A tabela "contas" está diferente do esperado. Rode o contas.sql de novo. Detalhe: ' + t.slice(0, 160);
  return 'Erro do banco: ' + t.slice(0, 200);
}
async function testaBanco() {
  if (DB.nome !== 'supabase') return DIAG;
  try {
    await sbReq('GET', '?select=uid&limit=1');
    DIAG = { ok: true, msg: 'Supabase funcionando ✅' };
    // a atualização v113 (guildas, denúncias, eventos, e-mail) já foi rodada?
    try {
      await sbReq('GET', '?select=email,guilda,reset,eventos&limit=1');
      await sbT('guildas', 'GET', '?select=id&limit=1');
      await sbT('denuncias', 'GET', '?select=id&limit=1');
      await sbT('config', 'GET', '?select=k&limit=1');
    } catch (e) {
      DIAG = { ok: false, msg: 'Falta rodar o arquivo atualizacao_v113.sql no SQL Editor do Supabase (guildas, denúncias, eventos e e-mail não funcionam sem ele).' };
      console.log('Contas (banco):', DIAG.msg);
      return DIAG;
    }
    try {
      await sbReq('GET', '?select=diamantes,itens_pagos,idade&limit=1');
      await sbT('pedidos', 'GET', '?select=id&limit=1');
    } catch (e) {
      DIAG = { ok: false, msg: 'Falta rodar o arquivo atualizacao_v115.sql no SQL Editor do Supabase (a loja de diamantes não funciona sem ele).' };
      console.log('Contas (banco):', DIAG.msg);
      return DIAG;
    }
  } catch (e) {
    DIAG = { ok: false, msg: explicaErro(e) };
  }
  console.log('Contas (banco):', DIAG.msg, '| URL usada:', SB_URL);
  return DIAG;
}
testaBanco().then(migraIds);

// ---- senha (scrypt com sal: nem quem abrir o banco vê a senha) ----
const scryptP = (s, salt) => new Promise((ok, no) => crypto.scrypt(s, salt, 32, { N: 16384, r: 8, p: 1 }, (e, k) => (e ? no(e) : ok(k))));
async function hashSenha(s) {
  const salt = crypto.randomBytes(16);
  return 's1$' + salt.toString('hex') + '$' + (await scryptP(s, salt)).toString('hex');
}
async function confereSenha(s, h) {
  const p = String(h || '').split('$');
  if (p.length !== 3 || p[0] !== 's1') return false;
  const k = await scryptP(s, Buffer.from(p[1], 'hex')),
    want = Buffer.from(p[2], 'hex');
  return want.length === k.length && crypto.timingSafeEqual(want, k);
}
const SENHA_FALSA = 's1$' + '0'.repeat(32) + '$' + '0'.repeat(64); // mesma demora quando o usuário não existe

// ---- sessão: "crachá" assinado, vale 60 dias ----
function fazToken(uid) {
  const body = Buffer.from(JSON.stringify({ u: uid, e: Date.now() + 60 * 864e5 })).toString('base64url');
  return body + '.' + crypto.createHmac('sha256', SEGREDO).update(body).digest('base64url');
}
function leToken(t) {
  if (typeof t !== 'string' || t.length > 400) return null;
  const [body, sig] = t.split('.');
  if (!body || !sig) return null;
  const want = crypto.createHmac('sha256', SEGREDO).update(body).digest(),
    got = Buffer.from(sig, 'base64url');
  if (got.length !== want.length || !crypto.timingSafeEqual(got, want)) return null;
  try {
    const o = JSON.parse(Buffer.from(body, 'base64url').toString());
    return o && typeof o.u === 'string' && o.e > Date.now() ? o.u : null;
  } catch (e) {
    return null;
  }
}

// ---- limites contra abuso ----
const RL = new Map();
function estourou(key, max, ms) {
  const now = Date.now();
  let r = RL.get(key);
  if (!r || now - r[0] > ms) RL.set(key, (r = [now, 0]));
  return ++r[1] > max;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, r] of RL) if (now - r[0] > 3600e3) RL.delete(k);
}, 600e3).unref();

const okUid = (s) => typeof s === 'string' && /^[1-9]\d{5,8}$/.test(s); // 6 números (antigos: 9)
const okUser = (s) => typeof s === 'string' && /^[a-z0-9_.]{3,16}$/.test(s);
const limpaTxt = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, n);
const limpaNome = (s) => limpaTxt(s, 14) || 'Jogador';
const objOk = (o, max) => !!o && typeof o === 'object' && !Array.isArray(o) && JSON.stringify(o).length <= max;
const arr = (a) => (Array.isArray(a) ? a : []);
// IDs em ordem de chegada: a 1ª conta é 100001, a 2ª 100002...
const ID6 = /^[1-9]\d{5}$/;
let PROX_ID = null;
async function proxId() {
  if (PROX_ID == null) {
    const r = await DB.maiorId();
    PROX_ID = Math.max(100001, (r ? +r : 100000) + 1);
  }
  return String(PROX_ID++);
}
// contas antigas (ID de 9 números) ganham ID novo na ordem em que foram criadas
async function migraIds() {
  try {
    const rows = await DB.todas();
    const velhas = rows.filter((r) => !ID6.test(r.uid));
    if (!velhas.length) return;
    let n = Math.max(100000, ...rows.filter((r) => ID6.test(r.uid)).map((r) => +r.uid)) + 1;
    velhas.sort((a, b) => (Date.parse(a.criado) || 0) - (Date.parse(b.criado) || 0) || (a.uid < b.uid ? -1 : 1));
    const M = {};
    for (const r of velhas) M[r.uid] = String(n++);
    const troca = (a) => arr(a).map((x) => M[x] || x);
    // 1) listas de amigos/pedidos de todo mundo  2) o ID de cada conta antiga
    for (const r of rows) {
      const L = [r.amigos, r.pedidos, r.enviados];
      if (L.some((a) => arr(a).some((x) => M[x]))) await DB.update(r.uid, { amigos: troca(r.amigos), pedidos: troca(r.pedidos), enviados: troca(r.enviados) });
    }
    for (const r of velhas) await DB.update(r.uid, { uid: M[r.uid], perfil: Object.assign({}, obj(r.perfil), { uid: M[r.uid], uidAntigo: r.uid }) });
    PROX_ID = null;
    RANK.t = 0;
    console.log('IDs novos:', velhas.map((r) => r.usuario + ' ' + r.uid + ' -> ' + M[r.uid]).join(', '));
  } catch (e) {
    console.warn('não deu para trocar os IDs agora:', e.message);
  }
}
const novoId = () => crypto.randomBytes(5).toString('hex');

// quem está conectado com qual conta (para amigos online e convites)
const porConta = new Map(); // uid -> Set(cliente)
function avisa(uid, msg) {
  const S = porConta.get(uid);
  if (S) for (const c of S) send(c, msg);
}
function onlineDe(uid) {
  const S = porConta.get(uid);
  if (!S || !S.size) return null;
  let st = 'lobby',
    sala = null;
  for (const c of S) {
    if (c.pr && (c.pr.st === 'play' || c.pr.st === 'dead' || c.pr.st === 'spec')) st = 'play';
    if (c.code) sala = c.code;
  }
  return { st, sala };
}
// v133: avatar, banner e personagem aparecem para os amigos (só o nome do item, nada pessoal)
const pfCampo = (r, k) => {
  const v = r && r.perfil && typeof r.perfil === 'object' ? r.perfil[k] : null;
  return typeof v === 'string' && /^[a-z0-9_]{1,30}$/.test(v) ? v : undefined;
};
const publico = (r) => ({ uid: r.uid, nome: r.nome, vf: r.vf | 0, on: !!onlineDe(r.uid), av: pfCampo(r, 'av'), bn: pfCampo(r, 'bn'), ch: pfCampo(r, 'ch') });
async function minha(r, full) {
  const am = await DB.many(arr(r.amigos)),
    pe = await DB.many(arr(r.pedidos)),
    en = await DB.many(arr(r.enviados));
  const o = {
    uid: r.uid,
    usuario: r.usuario,
    nome: r.nome,
    vf: r.vf | 0,
    admin: !!r.admin,
    acesso: arr(r.acesso),
    caixa: arr(r.caixa),
    amigos: am.map((a) => Object.assign(publico(a), { st: (onlineDe(a.uid) || {}).st || null, sala: (onlineDe(a.uid) || {}).sala || null })),
    pedidos: pe.map(publico),
    enviados: en.map(publico),
  };
  o.diamantes = n0(r.diamantes);
  o.itensPagos = arr(r.itens_pagos);
  o.idade = r.idade || null;
  o.email = mascara(r.email);
  o.recupera = MAIL_OK;
  if (r.guilda) {
    try {
      const g = await XB.g.get(r.guilda);
      if (g && cargoDe(g, r.uid)) o.guilda = { id: g.id, nome: g.nome, tag: g.tag, emblema: g.emblema, c: cargoDe(g, r.uid) };
    } catch (e) {}
  }
  if (full) {
    o.perfil = r.perfil || {};
    o.stats = r.stats || {};
  }
  return o;
}
class Erro extends Error {
  constructor(st, msg, extra) {
    super(msg);
    this.st = st;
    this.extra = extra; // v139: dados a mais na resposta (ex.: suspensão da conta)
  }
}
// v129: as roupas saíram do jogo — quem comprou com diamante recebe os diamantes de volta (uma vez)
const ROUPAS_VELHAS = { 'o:elfo': 25, 'o:noel': 30, 'o:deserto': 35, 'o:artico': 40, 'o:ops': 50, 'o:neon': 80, 'o:lenda': 150, 'o:cangaceiro': 350, 'o:samurai': 220, 'o:galaxia': 220, 'o:lava': 220, 'o:carnaval': 120, 'o:fantasma': 120, 'o:pirata': 120, 'o:amazonia': 120 };
async function devolveRoupas(r) {
  if (!r || !arr(r.itens_pagos).some((k) => String(k).startsWith('o:'))) return r;
  try {
    return await filaDm(r.uid, async () => {
      const a = await DB.get(r.uid); // lê de novo dentro da fila
      const ks = arr(a && a.itens_pagos).filter((k) => String(k).startsWith('o:'));
      if (!a || !ks.length) return a || r;
      const volta = ks.reduce((t, k) => t + (ROUPAS_VELHAS[k] || 0), 0);
      const a2 = await DB.update(a.uid, { diamantes: n0(a.diamantes) + volta, itens_pagos: arr(a.itens_pagos).filter((k) => !String(k).startsWith('o:')) });
      console.log('💎 roupas devolvidas', a.uid, ks.join(','), '+' + volta);
      return a2 || a;
    });
  } catch (e) {
    console.warn('devolveRoupas', e.message);
    return r;
  }
}
// a conta ganhou ID novo (6 números)? a sessão com o ID antigo ainda acha ela
async function contaDoToken(uid) {
  let r = await DB.get(uid);
  if (!r && !ID6.test(uid)) r = await DB.byOld(uid);
  return entregaGlobais(await devolveRoupas(r)); // v132: correio para todos
}
// ---------------- SUSPENSÃO DA CONTA (v139) ----------------
// Fica guardada em "eventos" como 'ban|<até quando>|<motivo>' (até = 0: permanente). Não precisa mudar o banco.
const BAN_MOTIVOS = { trapaca: 'Trapaça', toxico: 'Comportamento tóxico', arquivos: 'Alteração de arquivos/código' };
function banInfo(r) {
  if (!r || !r.banido) return null;
  const e = arr(r.eventos).find((x) => typeof x === 'string' && x.startsWith('ban|'));
  if (!e) return { ate: 0, motivo: '' }; // ban antigo: permanente
  const [, ate, motivo] = e.split('|');
  return { ate: +ate || 0, motivo: BAN_MOTIVOS[motivo] ? motivo : '' };
}
const semBan = (r) => arr(r.eventos).filter((x) => !(typeof x === 'string' && x.startsWith('ban|')));
// se a suspensão já acabou, libera a conta; se não, barra com os dados da suspensão
async function banChk(r) {
  const B = banInfo(r);
  if (!B) return r;
  if (B.ate && B.ate <= Date.now()) {
    const r2 = await DB.update(r.uid, { banido: false, eventos: semBan(r) });
    return r2 || Object.assign(r, { banido: false });
  }
  throw new Erro(403, B.ate ? 'Esta conta está suspensa temporariamente.' : 'Esta conta foi suspensa permanentemente.', { ban: B });
}
async function quem(b) {
  const uid = leToken(b.token);
  if (!uid) throw new Erro(401, 'Sua sessão expirou. Entre de novo.');
  const r = await contaDoToken(uid);
  if (!r) throw new Erro(401, 'Conta não encontrada. Entre de novo.');
  return banChk(r);
}
async function alvo(uid) {
  if (!okUid(uid)) throw new Erro(400, 'ID inválido (são 6 números).');
  const r = await DB.get(uid);
  if (!r) throw new Erro(404, 'Nenhum jogador com esse ID.');
  return r;
}
const sem = (a, x) => arr(a).filter((y) => y !== x);
const com = (a, x, max) => (arr(a).includes(x) ? arr(a) : arr(a).concat(x).slice(-max));
async function daPresente(r, item, de) {
  const caixa = arr(r.caixa).concat(Object.assign({ id: novoId(), de, t: Date.now() }, item)).slice(-50);
  await DB.update(r.uid, { caixa });
  avisa(r.uid, { t: 'ct' });
}

// ---- anti-trapaça: o servidor confere se os números que o jogo mandou são possíveis ----
// (moedas, partidas, abates, pontos do ranqueado e estrelas do 4v4). O que passar do limite é cortado.
const PRESENTE_MOEDAS = new Map(); // moedas que o Criador deu e ainda não apareceram no salvamento
const n0 = (v) => Math.max(0, Math.floor(+v || 0));
const obj = (v) => (v && typeof v === 'object' && !Array.isArray(v) ? v : {});
function confereStats(old, n, seg, uid) {
  const o = obj(old),
    sus = [];
  n.rk = obj(n.rk);
  n.rk4 = obj(n.rk4);
  const rkO = obj(o.rk),
    r4O = obj(o.rk4),
    maxM = Math.ceil(Math.max(0, seg) / 20) + 3; // no máximo 1 partida a cada 20 s (+ folga)
  const corta = (alvo, k, base, max, nome) => {
    if (n0(alvo[k]) - n0(base) > max) {
      alvo[k] = n0(base) + Math.max(0, max);
      sus.push(nome);
    }
  };
  corta(n, 'matches', o.matches, maxM, 'partidas');
  corta(n.rk, 'm', rkO.m, maxM, 'partidas ranqueadas');
  corta(n.rk4, 'm', r4O.m, maxM, 'partidas 4v4');
  const dm = Math.max(1, n0(n.matches) - n0(o.matches)),
    dr = Math.max(0, n0(n.rk.m) - n0(rkO.m)),
    d4 = Math.max(0, n0(n.rk4.m) - n0(r4O.m));
  corta(n, 'rp', o.rp, dr * 100 + 5, 'pontos do ranqueado'); // no máximo ~98 RP por partida
  corta(n, 'st4', o.st4, d4 * 2, 'estrelas do 4v4'); // no máximo 2 estrelas por vitória
  corta(n, 'kills', o.kills, dm * 40, 'abates');
  corta(n, 'wins', o.wins, dm, 'vitórias');
  const presente = PRESENTE_MOEDAS.get(uid) || 0;
  corta(n, 'coins', o.coins, (6000 + dm * 2500) * fatorMoedas() + presente, 'moedas');
  PRESENTE_MOEDAS.delete(uid);
  if (n0(n.wins) > n0(n.matches)) n.wins = n0(n.matches);
  n._sus = n0(o._sus) + (sus.length ? 1 : 0);
  if (o._ev) n._ev = o._ev; // quantas vezes já foi cortado (o Criador vê)
  if (sus.length) console.warn('anti-trapaça', uid, sus.join(', '));
  return sus;
}
// conta nova que já traz progresso de convidado: números precisam bater com as partidas jogadas
function confereNovo(st) {
  st.rk = obj(st.rk);
  st.rk4 = obj(st.rk4);
  const m = n0(st.matches);
  if (n0(st.rk.m) > m) st.rk.m = m;
  if (n0(st.rk4.m) > m) st.rk4.m = m;
  st.rp = Math.min(n0(st.rp), n0(st.rk.m) * 100);
  st.st4 = Math.min(n0(st.st4), n0(st.rk4.m) * 2);
  st.kills = Math.min(n0(st.kills), m * 40);
  st.wins = Math.min(n0(st.wins), m);
  st.coins = Math.min(n0(st.coins), 20000 + m * 2500);
  st._sus = 0;
  return st;
}

// =====================================================================
//  LOJA DE DIAMANTES 💎 — Pix e cartão pelo Mercado Pago
//  Os diamantes ficam SÓ no servidor: só entram depois que o Mercado Pago
//  confirma o pagamento, e só saem quando a pessoa compra um item escolhido.
//  (Nada aleatório com diamante: o Royale continua só com moedas ganhas jogando.)
// =====================================================================
const MP_TOKEN = envLimpo('MP_TOKEN'),
  MP_API = (envLimpo('MP_API') || 'https://api.mercadopago.com').replace(/\/+$/, ''),
  MP_OK = !!MP_TOKEN,
  SITE_URL = envLimpo('SITE_URL').replace(/\/+$/, '');
// pacotes à venda (mude os preços aqui se quiser)
const PACOTES = [
  { id: 'd100', d: 100, bonus: 0, preco: 2.99 },
  { id: 'd310', d: 300, bonus: 10, preco: 7.99 },
  { id: 'd520', d: 500, bonus: 20, preco: 12.99 },
  { id: 'd1060', d: 1000, bonus: 60, preco: 24.99 },
  { id: 'd2180', d: 2000, bonus: 180, preco: 49.99 },
];
// itens que dá para comprar com diamantes (escolhe e sabe o que leva) — [preço em 💎, nome]
const ITENS_D = {
  'ch:q_casual': [80, 'Casual'],
  'ch:q_estilo': [80, 'Estiloso'],
  'ch:q_praia': [80, 'Praiano'],
  'ch:q_fazendeiro': [120, 'Fazendeiro'],
  'ch:q_peao': [120, 'Peão de Obra'],
  'ch:q_punk': [150, 'Punk'],
  'ch:q_aventureiro': [200, 'Aventureiro'],
  'ch:q_agente': [250, 'Agente'],
  'ch:q_rei': [400, 'Rei'],
  'ch:q_astronauta': [400, 'Astronauta'],
  'ch:f_casual': [80, 'Descolada'],
  'ch:f_social': [80, 'Social'],
  'ch:f_executiva': [120, 'Executiva'],
  'ch:f_operaria': [120, 'Operária'],
  'ch:f_punk': [150, 'Roqueira'],
  'ch:f_aventureira': [200, 'Aventureira'],
  'ch:f_scifi': [250, 'Sci-Fi'],
  'ch:f_medieval': [300, 'Medieval'],
  'ch:f_bruxa': [400, 'Bruxa'],
  'g:camo': [20, "Camuflada"],
  'g:carbono': [35, "Carbono"],
  'g:neve': [30, "Neve"],
  'g:doce': [40, "Doce de Natal"],
  'g:rosa': [70, "Neon Rosa"],
  'g:ouro': [120, "Ouro"],
  'g:dragao': [350, "Dragão"],
  'g:arcoiris': [350, "Arco-íris"],
  'g:nebulosa': [220, "Nebulosa"],
  'g:magma': [220, "Magma"],
  'g:cromo': [220, "Cromo"],
  'g:gelo': [120, "Gelo"],
  'g:tigre': [120, "Tigre"],
  'g:esmeralda': [120, "Esmeralda"],
  'p:pc_ceu': [20, "Céu"],
  'p:pc_brasil': [30, "Brasil"],
  'p:pc_noite': [35, "Noite"],
  'p:pc_galaxia': [120, "Galáxia"],
  'p:pc_fogo': [220, "Fogo"],
  'p:pc_ouro': [220, "Ouro"],
  'p:pc_arcoiris': [350, "Arco-íris"],
  'e:dab': [20, 'Dab'],
  'e:chorar': [20, 'Chorão'],
  'e:biceps': [25, 'Marombeiro'],
  'e:passinho': [30, 'Passinho'],
  'e:robo': [30, 'Robô'],
  'e:flexao': [35, 'Flexão'],
  'e:ginga': [40, 'Ginga de Capoeira'],
  'e:samba': [40, 'Samba no Pé'],
  'e:frevo': [70, 'Frevo'],
};
const pacoteDe = (id) => PACOTES.find((p) => p.id === id);
// uma mexida de diamantes por vez por conta (duas compras juntas não somem com nada)
const FILA_DM = new Map();
function filaDm(uid, fn) {
  const ant = FILA_DM.get(uid) || Promise.resolve();
  const p = ant.then(fn, fn);
  const fim = p.catch(() => {});
  FILA_DM.set(uid, fim);
  fim.then(() => FILA_DM.get(uid) === fim && FILA_DM.delete(uid));
  return p;
}
async function mp(method, caminho, body, idem) {
  const h = { Authorization: 'Bearer ' + MP_TOKEN, 'Content-Type': 'application/json', Accept: 'application/json' };
  if (idem) h['X-Idempotency-Key'] = idem;
  const r = await fetch(MP_API + caminho, { method, headers: h, body: body ? JSON.stringify(body) : undefined });
  const t = await r.text();
  let j = null;
  try {
    j = t ? JSON.parse(t) : null;
  } catch (e) {}
  if (!r.ok) {
    const e = new Error('mercadopago ' + r.status + ' ' + t.slice(0, 300));
    e.status = r.status;
    throw e;
  }
  return j;
}
function siteBase(req) {
  if (SITE_URL) return SITE_URL;
  const host = String((req && (req.headers['x-forwarded-host'] || req.headers.host)) || '').split(',')[0].trim();
  const proto = String((req && req.headers['x-forwarded-proto']) || 'https').split(',')[0].trim();
  return host ? proto + '://' + host : '';
}
// pagamento aprovado: entrega os diamantes (uma vez só por pedido)
async function creditaPedido(ped, pg) {
  if (!ped || ped.status === 'pago') return false;
  if (!pg || pg.status !== 'approved') return false;
  if (String(pg.external_reference) !== String(ped.id)) return false;
  if (Math.abs(+pg.transaction_amount - +ped.valor) > 0.01) {
    console.warn('pagamento com valor diferente', ped.id, pg.transaction_amount, ped.valor);
    return false;
  }
  const marcou = await XB.p.marcaPago(ped.id, String(pg.id)); // só um "ganha" a corrida
  if (!marcou) return false;
  const ok = await filaDm(ped.uid, async () => {
    const r = await DB.get(ped.uid);
    if (!r) return false;
    await DB.update(ped.uid, { diamantes: n0(r.diamantes) + n0(ped.diamantes) });
    return true;
  });
  if (!ok) return false;
  avisa(ped.uid, { t: 'ct', dm: { n: n0(ped.diamantes) } });
  console.log('💎 venda', ped.id, ped.uid, ped.diamantes, 'R$', ped.valor);
  return true;
}
async function confereNoMP(ped) {
  if (!ped || ped.status === 'pago' || !MP_OK) return ped;
  try {
    const j = await mp('GET', '/v1/payments/search?external_reference=' + enc(ped.id) + '&sort=date_created&criteria=desc');
    for (const pg of (j && j.results) || []) if (await creditaPedido(ped, pg)) return Object.assign({}, ped, { status: 'pago' });
  } catch (e) {
    console.warn('mercadopago busca', e.message);
  }
  return ped;
}
// aviso do Mercado Pago (webhook): sempre confere o pagamento direto na API (ninguém consegue fingir)
async function mpWebhook(req, res, url) {
  let body = '';
  req.on('data', (d) => {
    if (body.length < 20000) body += d;
  });
  req.on('end', async () => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    try {
      const q = new URL(url, 'http://x').searchParams;
      let id = q.get('data.id') || q.get('id'),
        tipo = q.get('type') || q.get('topic');
      try {
        const j = JSON.parse(body || '{}');
        if (j && j.data && j.data.id) id = j.data.id;
        if (j && j.type) tipo = j.type;
      } catch (e) {}
      if (!id || (tipo && tipo !== 'payment')) return;
      if (!/^\d{1,20}$/.test(String(id))) return;
      const pg = await mp('GET', '/v1/payments/' + id);
      const ped = pg && pg.external_reference ? await XB.p.get(String(pg.external_reference)) : null;
      if (ped) await creditaPedido(ped, pg);
    } catch (e) {
      console.warn('webhook mercadopago', e.message);
    }
  });
}

const RANK = { t: 0, br: null, cs: null, p: null }; // ranking guardado por 45 s (não pesa no banco)
// =====================================================================
//  v113: DENÚNCIAS, RECUPERAR SENHA POR E-MAIL, EVENTOS E GUILDAS
// =====================================================================
// ---- ranking carregado (também serve para os pontos das guildas) ----
async function carregaRank() {
  if (Date.now() - RANK.t < 45e3 && RANK.br) return;
  if (!RANK.p)
    RANK.p = DB.ranking()
      .then((rows) => {
        const L = (rows || [])
          .filter((r) => r && !r.banido)
          .map((r) => ({ uid: r.uid, nome: r.nome, vf: r.vf | 0, g: r.guilda || null, rp: Math.max(0, +r.rp || 0), st4: Math.max(0, +r.st4 || 0), wins: +r.wins || 0, kills: +r.kills || 0 }));
        RANK.br = L.filter((r) => r.rp > 0).sort((a, b) => b.rp - a.rp || b.wins - a.wins);
        RANK.cs = L.filter((r) => r.st4 > 0).sort((a, b) => b.st4 - a.st4 || b.wins - a.wins);
        RANK.pts = {};
        for (const r of L) RANK.pts[r.uid] = r.rp + r.st4 * 40;
        RANK.t = Date.now();
      })
      .finally(() => (RANK.p = null));
  await RANK.p;
}
// ---- guildas em cache (tags no ranking e na busca) ----
const GC = { t: 0, list: [], by: {} };
async function guildasCache(forca) {
  if (!forca && Date.now() - GC.t < 30e3) return GC;
  try {
    GC.list = (await XB.g.list()) || [];
  } catch (e) {
    GC.list = GC.list || [];
  }
  GC.by = {};
  for (const g of GC.list) GC.by[g.id] = g;
  GC.t = Date.now();
  return GC;
}
const gTag = (id) => (id && GC.by[id] ? GC.by[id].tag : null);

// ---------------- DENÚNCIAS ----------------
const MOTIVOS = { trapaca: 'Trapaça / hack', nome: 'Nome ofensivo', ofensa: 'Ofensa / preconceito', antijogo: 'Atrapalhando o time', outro: 'Outro' };
// ---------------- E-MAIL (Brevo) ----------------
const BREVO_KEY = envLimpo('BREVO_KEY'),
  EMAIL_DE = envLimpo('EMAIL_DE').toLowerCase(),
  BREVO_URL = envLimpo('BREVO_URL') || 'https://api.brevo.com/v3/smtp/email',
  MAIL_OK = !!(BREVO_KEY && EMAIL_DE);
const okEmail = (e) => typeof e === 'string' && e.length <= 80 && /^[^\s@<>]+@[^\s@<>]+\.[a-z]{2,}$/i.test(e);
const mascara = (e) => (e ? String(e).replace(/^(.)(.*)(@.*)$/, (m, a, b, c) => a + '*'.repeat(Math.min(6, b.length || 1)) + c) : null);
async function mandaEmail(para, assunto, html) {
  const r = await fetch(BREVO_URL, {
    method: 'POST',
    headers: { 'api-key': BREVO_KEY, 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ sender: { name: 'WorldBullet', email: EMAIL_DE }, to: [{ email: para }], subject: assunto, htmlContent: html }),
  });
  if (!r.ok) throw new Error('email ' + r.status + ' ' + (await r.text()).slice(0, 200));
}
const hashCodigo = (uid, cod) => crypto.createHmac('sha256', SEGREDO).update('reset|' + uid + '|' + cod).digest('hex');
// ---------------- EVENTOS ----------------
const EV = { t: 0, list: [] };
async function eventosAtivos(forca) {
  if (forca || Date.now() - EV.t > 20e3) {
    try {
      EV.list = arr(await XB.c.get('eventos'));
    } catch (e) {}
    EV.t = Date.now();
  }
  const now = Date.now();
  return EV.list.filter((e) => e && e.fim > now && e.ini <= now);
}
// ---------------- CORREIO PARA TODOS (v132) ----------------
// O Criador manda uma vez; cada jogador recebe no correio quando entra no jogo (até a data de fim).
// Quem já recebeu fica marcado em "eventos" com 'cg:' na frente (não precisa mudar o banco).
const CG = { t: 0, list: [] };
async function correioGlobal(forca) {
  if (forca || Date.now() - CG.t > 20e3) {
    try {
      CG.list = arr(await XB.c.get('correio'));
    } catch (e) {}
    CG.t = Date.now();
  }
  const now = Date.now();
  return CG.list.filter((x) => x && x.fim > now && x.ini <= now);
}
async function entregaGlobais(r) {
  if (!r || r.banido) return r;
  let L;
  try {
    L = await correioGlobal();
  } catch (e) {
    return r;
  }
  if (!L.length || !L.some((x) => !arr(r.eventos).includes('cg:' + x.id))) return r;
  try {
    return await filaDm(r.uid, async () => {
      const a = await DB.get(r.uid); // lê de novo dentro da fila (não entrega duas vezes)
      if (!a) return r;
      const ja = arr(a.eventos),
        nv = L.filter((x) => !ja.includes('cg:' + x.id));
      if (!nv.length) return a;
      const caixa = arr(a.caixa)
        .concat(nv.map((x) => Object.assign({ id: novoId(), de: x.de || 'WorldBullet', t: Date.now() }, x.item)))
        .slice(-50);
      const a2 = await DB.update(a.uid, { caixa, eventos: ja.concat(nv.map((x) => 'cg:' + x.id)).slice(-200) });
      return a2 || Object.assign(a, { caixa });
    });
  } catch (e) {
    console.warn('entregaGlobais', e.message);
    return r;
  }
}
const cgPub = (x) => ({ id: x.id, ini: x.ini, fim: x.fim, de: x.de, tipo: x.item.tipo, msg: x.item.msg || '', premio: x.item.tipo === 'moedas' ? (x.item.n | 0) + ' moedas' : x.item.tipo === 'item' ? x.item.nome || x.item.item : 'Mensagem' });
const fatorMoedas = () => (EV.list.some((e) => e && e.tipo === 'moedas2x' && e.fim > Date.now() && e.ini <= Date.now()) ? 2 : 1);
const evPub = (e) => ({ id: e.id, tipo: e.tipo, nome: e.nome, ini: e.ini, fim: e.fim, premio: e.tipo === 'presente' ? (e.item ? e.item.nome : (e.n | 0) + ' moedas') : null });
// ---------------- GUILDAS ----------------
const EMBLEMAS = ['🛡️', '⚔️', '🔥', '🐊', '🦅', '💀', '👑', '⚡', '🌊', '🌴', '🐆', '🎯', '🐍', '🦈', '🌙', '⭐'];
const GMAX = 30;
const chaveG = (n) =>
  String(n)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
const cargoDe = (g, uid) => (arr(g.membros).find((m) => m.u === uid) || {}).c || null;
const gPontos = (g) => arr(g.membros).reduce((t, m) => t + ((RANK.pts || {})[m.u] || 0), 0);
async function gAvisa(g, msg, so) {
  for (const m of arr(g.membros)) if (!so || so.includes(m.c)) avisa(m.u, msg);
}
async function gDetalhe(g, eu) {
  await carregaRank();
  const ids = arr(g.membros).map((m) => m.u),
    P = await DB.many(ids),
    by = {};
  for (const p of P) by[p.uid] = p;
  const ordem = { lider: 0, vice: 1, membro: 2 };
  const membros = arr(g.membros)
    .filter((m) => by[m.u])
    .map((m) => ({ uid: m.u, nome: by[m.u].nome, vf: by[m.u].vf | 0, c: m.c, on: !!onlineDe(m.u), pts: (RANK.pts || {})[m.u] || 0 }))
    .sort((a, b) => ordem[a.c] - ordem[b.c] || b.pts - a.pts);
  const o = { id: g.id, nome: g.nome, tag: g.tag, emblema: g.emblema, descr: g.descr || '', aberta: !!g.aberta, n: membros.length, max: GMAX, pontos: gPontos(g), membros };
  const c = eu ? cargoDe(g, eu) : null;
  if (c) {
    o.eu = c;
    o.chat = arr(g.chat).slice(-40);
    if (c === 'lider' || c === 'vice') o.pedidos = (await DB.many(arr(g.pedidos))).map(publico);
  }
  return o;
}
// tira alguém da guilda (sair, expulso ou conta excluída)
async function gTira(g, uid, motivo) {
  const resto = arr(g.membros).filter((m) => m.u !== uid);
  await DB.update(uid, { guilda: null }).catch(() => {});
  if (!resto.length) {
    await XB.g.remove(g.id);
    GC.t = 0;
    return null;
  }
  if (cargoDe(g, uid) === 'lider') {
    const nv = resto.find((m) => m.c === 'vice') || resto[0];
    nv.c = 'lider';
  }
  const chat = arr(g.chat)
    .concat({ s: 1, m: motivo, t: Date.now() })
    .slice(-40);
  const ng = await XB.g.update(g.id, { membros: resto, chat });
  GC.t = 0;
  gAvisa(ng || g, { t: 'gch' });
  return ng;
}

const API = {
  async registrar(b, ip) {
    if (estourou('reg:' + ip, 6, 3600e3)) throw new Erro(429, 'Muitas contas criadas daqui. Tente mais tarde.');
    const usuario = String(b.usuario || '').toLowerCase().trim(),
      senha = String(b.senha || '');
    if (!okUser(usuario)) throw new Erro(400, 'Usuário: 3 a 16 letras minúsculas, números, _ ou .');
    if (senha.length < 6 || senha.length > 64) throw new Erro(400, 'A senha precisa ter de 6 a 64 caracteres.');
    const perfil = objOk(b.perfil, 65536) ? b.perfil : {},
      stats = confereNovo(objOk(b.stats, 65536) ? b.stats : {});
    if (await DB.byUser(usuario)) throw new Erro(409, 'Esse nome de usuário já existe.');
    const h = await hashSenha(senha),
      adm = !!ADMIN && usuario === ADMIN;
    let uid = null;
    for (let i = 0; i < 12; i++) {
      if (!uid) uid = await proxId(); // próximo ID da fila
      perfil.uid = uid;
      const row = contaNova({ uid, usuario, nome: limpaNome(b.nome || perfil.name), senha: h, perfil, stats, vf: adm ? 2 : 0, admin: adm });
      const r = await DB.insert(row);
      if (r) return { token: fazToken(uid), conta: await minha(r, true) };
      if (await DB.byUser(usuario)) throw new Erro(409, 'Esse nome de usuário já existe.');
      uid = null;
    }
    throw new Erro(500, 'Não deu para criar a conta agora. Tente de novo.');
  },
  async entrar(b, ip) {
    if (estourou('ent:' + ip, 30, 600e3)) throw new Erro(429, 'Muitas tentativas. Espere uns minutos.');
    const usuario = String(b.usuario || '').toLowerCase().trim(),
      senha = String(b.senha || '').slice(0, 64);
    if (RL.has('falha:' + usuario) && RL.get('falha:' + usuario)[1] >= 8 && Date.now() - RL.get('falha:' + usuario)[0] < 900e3)
      throw new Erro(429, 'Muitas senhas erradas nessa conta. Espere 15 minutos.');
    let r = okUser(usuario) ? await DB.byUser(usuario) : null;
    const ok = await confereSenha(senha, r ? r.senha : SENHA_FALSA);
    if (!r || !ok) {
      estourou('falha:' + usuario, 1e9, 900e3);
      throw new Erro(401, 'Usuário ou senha errados.');
    }
    RL.delete('falha:' + usuario);
    r = await banChk(r); // v139: suspensa? mostra o tempo e o motivo
    const patch = { visto: agora() };
    if (ADMIN && usuario === ADMIN && !r.admin) Object.assign(patch, { admin: true, vf: 2 });
    r = (await DB.update(r.uid, patch)) || r;
    return { token: fazToken(r.uid), conta: await minha(r, true) };
  },
  // v134: qual é a versão do jogo agora (o jogo aberto confere e mostra "atualização disponível")
  async versao() {
    return { v: GAME_VER, rein: REIN.ate > Date.now() ? REIN.ate : 0 };
  },
  async eu(b) {
    const r = await quem(b),
      o = { conta: await minha(r, !!b.full) };
    if (leToken(b.token) !== r.uid) o.token = fazToken(r.uid); // ID mudou: crachá novo
    return o;
  },
  async salvar(b) {
    const r = await quem(b);
    if (estourou('sv:' + r.uid, 30, 60e3)) throw new Erro(429, 'Calma: salvando rápido demais.');
    if (!objOk(b.perfil, 65536) || !objOk(b.stats, 65536)) throw new Erro(400, 'Progresso grande demais.');
    b.perfil.uid = r.uid;
    const seg = (Date.now() - (Date.parse(r.visto) || 0)) / 1000,
      cortou = confereStats(r.stats, b.stats, seg, r.uid);
    await DB.update(r.uid, { perfil: b.perfil, stats: b.stats, nome: limpaNome(b.perfil.name), visto: agora() });
    return { ok: true, ajuste: cortou.length ? { rp: b.stats.rp, st4: b.stats.st4, coins: b.stats.coins, matches: b.stats.matches, kills: b.stats.kills, wins: b.stats.wins } : null };
  },
  async online(b) {
    const r = await quem(b);
    if (estourou('ol:' + r.uid, 20, 60e3)) throw new Erro(429, 'Calma, espere um pouquinho para atualizar.');
    const ids = [...porConta.keys()].filter((u) => u !== r.uid && porConta.get(u) && porConta.get(u).size);
    const total = ids.length;
    // quem está no lobby primeiro (dá pra chamar), depois quem está jogando
    ids.sort((a, b) => (onlineDe(a).st === 'play') - (onlineDe(b).st === 'play'));
    const L = await DB.onlineInfo(ids.slice(0, 80));
    const am = new Set(arr(r.amigos)),
      en = new Set(arr(r.enviados)),
      pe = new Set(arr(r.pedidos));
    const jog = L.filter((x) => x && !x.banido && !(x.oculto === true || x.oculto === 'true'))
      .map((x) => {
        const o = onlineDe(x.uid) || { st: 'lobby' };
        return { uid: x.uid, nome: x.nome, vf: x.vf | 0, st: o.st, rp: n0(x.rp), st4: n0(x.st4), rel: am.has(x.uid) ? 'amigo' : en.has(x.uid) ? 'enviado' : pe.has(x.uid) ? 'pediu' : '' };
      })
      .sort((a, b) => (a.st === 'play') - (b.st === 'play') || (b.vf | 0) - (a.vf | 0) || b.rp - a.rp)
      .slice(0, 60);
    return { jogadores: jog, total };
  },
  async jogador(b) {
    const a = await alvo(String(b.uid || '').replace(/\D/g, ''));
    return { jogador: publico(a) };
  },
  async amigos(b) {
    const r = await quem(b),
      ac = String(b.acao || '');
    if (estourou('am:' + r.uid, 40, 60e3)) throw new Erro(429, 'Calma aí, muitas ações seguidas.');
    const a = await alvo(String(b.uid || ''));
    if (a.uid === r.uid) throw new Erro(400, 'Esse é o seu próprio ID 😅');
    let msg = '';
    if (ac === 'pedir') {
      if (arr(r.amigos).includes(a.uid)) msg = 'Vocês já são amigos.';
      else if (arr(r.pedidos).includes(a.uid)) return API.amigos(Object.assign({}, b, { acao: 'aceitar' }));
      else {
        if (arr(r.amigos).length >= 100) throw new Erro(400, 'Você chegou no limite de 100 amigos.');
        await DB.update(a.uid, { pedidos: com(a.pedidos, r.uid, 50) });
        await DB.update(r.uid, { enviados: com(r.enviados, a.uid, 50) });
        avisa(a.uid, { t: 'ct', pedido: { uid: r.uid, nome: r.nome } });
        msg = 'Pedido enviado para ' + a.nome + '!';
      }
    } else if (ac === 'aceitar') {
      if (!arr(r.pedidos).includes(a.uid)) throw new Erro(400, 'Esse pedido não existe mais.');
      await DB.update(r.uid, { pedidos: sem(r.pedidos, a.uid), enviados: sem(r.enviados, a.uid), amigos: com(r.amigos, a.uid, 100) });
      await DB.update(a.uid, { enviados: sem(a.enviados, r.uid), pedidos: sem(a.pedidos, r.uid), amigos: com(a.amigos, r.uid, 100) });
      avisa(a.uid, { t: 'ct', aceito: { uid: r.uid, nome: r.nome } });
      msg = 'Agora você e ' + a.nome + ' são amigos!';
    } else if (ac === 'recusar' || ac === 'cancelar') {
      await DB.update(r.uid, { pedidos: sem(r.pedidos, a.uid), enviados: sem(r.enviados, a.uid) });
      await DB.update(a.uid, { pedidos: sem(a.pedidos, r.uid), enviados: sem(a.enviados, r.uid) });
      avisa(a.uid, { t: 'ct' });
    } else if (ac === 'remover') {
      await DB.update(r.uid, { amigos: sem(r.amigos, a.uid) });
      await DB.update(a.uid, { amigos: sem(a.amigos, r.uid) });
      avisa(a.uid, { t: 'ct' });
      msg = a.nome + ' saiu da sua lista.';
    } else throw new Erro(400, 'Ação desconhecida.');
    return { msg, conta: await minha(await DB.get(r.uid)) };
  },
  async convidar(b) {
    const r = await quem(b);
    if (estourou('cv:' + r.uid, 20, 60e3)) throw new Erro(429, 'Muitos convites seguidos. Espere um pouco.');
    if (!okCode(b.sala)) throw new Erro(400, 'Sala inválida.');
    if (!arr(r.amigos).includes(String(b.uid))) throw new Erro(403, 'Só dá para convidar amigos.');
    if (!onlineDe(String(b.uid))) throw new Erro(409, 'Seu amigo não está online agora.');
    avisa(String(b.uid), { t: 'conv', id: novoId(), de: { uid: r.uid, nome: r.nome, vf: r.vf | 0 }, sala: b.sala });
    return { ok: true };
  },
  async ranking(b) {
    await carregaRank();
    await guildasCache();
    const uid = leToken(b.token),
      top = (A) => A.slice(0, 100).map((r) => Object.assign({}, r, { on: !!onlineDe(r.uid), tag: gTag(r.g) })),
      pos = (A) => (uid ? A.findIndex((r) => r.uid === uid) + 1 : 0);
    return { br: top(RANK.br), cs: top(RANK.cs), eu: uid ? { br: pos(RANK.br), cs: pos(RANK.cs) } : null, total: RANK.br.length, t: RANK.t };
  },
  async resgatar(b) {
    const r = await quem(b),
      cx = arr(r.caixa),
      pega = b.id === '*' ? cx : cx.filter((x) => x.id === b.id);
    if (!pega.length) return { itens: [] };
    await DB.update(r.uid, { caixa: cx.filter((x) => !pega.includes(x)) });
    const mo = pega.reduce((t, x) => t + (x.tipo === 'moedas' ? n0(x.n) : 0), 0);
    if (mo) PRESENTE_MOEDAS.set(r.uid, (PRESENTE_MOEDAS.get(r.uid) || 0) + mo);
    return { itens: pega };
  },
  // ---------- loja de diamantes ----------
  async loja(b) {
    const uid = leToken(b.token);
    const r = uid ? await contaDoToken(uid).catch(() => null) : null;
    return { ligada: MP_OK, pacotes: PACOTES.map((p) => Object.assign({ total: p.d + p.bonus }, p)), itens: ITENS_D, diamantes: r ? n0(r.diamantes) : 0, itensPagos: r ? arr(r.itens_pagos) : [], idade: r ? r.idade || null : null };
  },
  async idade(b) {
    const r = await quem(b),
      t = b.tipo === '18' ? '18' : b.tipo === 'resp' ? 'resp' : null;
    if (!t) throw new Erro(400, 'Escolha uma opção.');
    await DB.update(r.uid, { idade: t });
    return { ok: true, idade: t };
  },
  async comprar(b, ip, req) {
    const r = await quem(b);
    if (!MP_OK) throw new Erro(503, 'A loja ainda não foi aberta pelo Criador.');
    if (!r.idade) throw new Erro(403, 'Confirme sua idade antes de comprar.');
    const pk = pacoteDe(String(b.pacote || ''));
    if (!pk) throw new Erro(400, 'Pacote inválido.');
    if (estourou('cp:' + r.uid, 8, 600e3)) throw new Erro(429, 'Muitos pedidos seguidos. Espere uns minutos.');
    const base = siteBase(req),
      metodo = b.metodo === 'cartao' ? 'cartao' : 'pix',
      id = 'p' + Date.now().toString(36) + crypto.randomBytes(3).toString('hex'),
      titulo = 'WorldBullet · ' + (pk.d + pk.bonus) + ' diamantes';
    await XB.p.insert({ id, uid: r.uid, pacote: pk.id, diamantes: pk.d + pk.bonus, valor: pk.preco, metodo, status: 'aberto', criado: agora() });
    const notif = base ? base + '/mp/webhook' : undefined;
    try {
      if (metodo === 'pix') {
        const pg = await mp(
          'POST',
          '/v1/payments',
          {
            transaction_amount: pk.preco,
            description: titulo,
            payment_method_id: 'pix',
            external_reference: id,
            notification_url: notif,
            date_of_expiration: new Date(Date.now() + 30 * 60e3 - 3 * 3600e3).toISOString().replace('Z', '-03:00'), // horário de Brasília
            payer: { email: r.email || 'jogador' + r.uid + '@worldbullet.app', first_name: limpaTxt(r.nome, 30) || 'Jogador' },
          },
          id,
        );
        const td = (pg && pg.point_of_interaction && pg.point_of_interaction.transaction_data) || {};
        await XB.p.update(id, { mp_id: String(pg.id) });
        return { pedido: id, metodo, qr: td.qr_code || '', qr64: td.qr_code_base64 || '', link: td.ticket_url || '', valor: pk.preco, diamantes: pk.d + pk.bonus, expira: Date.now() + 30 * 60e3 };
      }
      const pref = await mp(
        'POST',
        '/checkout/preferences',
        {
          items: [{ id: pk.id, title: titulo, quantity: 1, currency_id: 'BRL', unit_price: pk.preco }],
          external_reference: id,
          notification_url: notif,
          back_urls: base ? { success: base + '/?compra=ok', pending: base + '/?compra=espera', failure: base + '/?compra=erro' } : undefined,
          auto_return: base ? 'approved' : undefined,
          payment_methods: { excluded_payment_types: [{ id: 'ticket' }] },
        },
        id,
      );
      return { pedido: id, metodo, link: pref.init_point, valor: pk.preco, diamantes: pk.d + pk.bonus };
    } catch (e) {
      console.warn('mercadopago criar', e.message);
      await XB.p.update(id, { status: 'erro' });
      throw new Erro(503, 'O Mercado Pago não respondeu. Tente de novo em instantes.');
    }
  },
  async pedido(b) {
    const r = await quem(b);
    let ped = await XB.p.get(String(b.pedido || ''));
    if (!ped || ped.uid !== r.uid) throw new Erro(404, 'Pedido não encontrado.');
    if (ped.status !== 'pago' && estourou('pd:' + r.uid, 40, 60e3) === false) ped = await confereNoMP(ped);
    const r2 = await DB.get(r.uid);
    return { status: ped.status, diamantes: n0(r2 && r2.diamantes) };
  },
  async compras(b) {
    const r = await quem(b);
    const L = await XB.p.lista(r.uid);
    return { compras: L.filter((q) => q.status === 'pago').map((q) => ({ id: q.id, diamantes: q.diamantes, valor: q.valor, pago: q.pago || q.criado })) };
  },
  async gastar(b) {
    const r0 = await quem(b),
      k = String(b.cat || '') + ':' + String(b.id || ''),
      it = ITENS_D[k];
    if (!it) throw new Erro(400, 'Esse item não está à venda com diamantes.');
    if (estourou('gs:' + r0.uid, 30, 60e3)) throw new Erro(429, 'Calma, muitas compras seguidas.');
    return filaDm(r0.uid, async () => {
      const r = await DB.get(r0.uid); // lê de novo dentro da fila
      if (arr(r.itens_pagos).includes(k)) return { ok: true, diamantes: n0(r.diamantes), ja: true };
      if (n0(r.diamantes) < it[0]) throw new Erro(400, 'Diamantes insuficientes.');
      const r2 = await DB.update(r.uid, { diamantes: n0(r.diamantes) - it[0], itens_pagos: arr(r.itens_pagos).concat(k) });
      console.log('💎 gasto', r.uid, k, it[0]);
      return { ok: true, diamantes: n0(r2 && r2.diamantes), item: k, nome: it[1] };
    });
  },
  // ---------- denunciar ----------
  async denunciar(b) {
    const r = await quem(b),
      a = await alvo(String(b.uid || '').replace(/\D/g, ''));
    if (a.uid === r.uid) throw new Erro(400, 'Não dá para denunciar você mesmo 😅');
    const motivo = String(b.motivo || '');
    if (!MOTIVOS[motivo]) throw new Erro(400, 'Escolha um motivo.');
    if (estourou('dn:' + r.uid + ':' + a.uid, 1, 864e5)) throw new Erro(429, 'Você já denunciou esse jogador hoje. O Criador vai olhar.');
    if (estourou('dn:' + r.uid, 10, 3600e3)) throw new Erro(429, 'Muitas denúncias seguidas. Espere um pouco.');
    await XB.d.insert({ alvo: a.uid, autor: r.uid, motivo, detalhe: limpaTxt(b.detalhe, 200), mid: limpaTxt(b.mid, 20) || null, status: 'aberta' });
    return { ok: true, msg: 'Denúncia enviada. Obrigado por ajudar! 🚩' };
  },
  // ---------- e-mail e recuperar senha ----------
  async email(b) {
    const r = await quem(b);
    if (!(await confereSenha(String(b.senha || '').slice(0, 64), r.senha))) throw new Erro(401, 'Senha errada.');
    const e = String(b.email || '').trim().toLowerCase();
    if (e && !okEmail(e)) throw new Erro(400, 'E-mail inválido.');
    await DB.update(r.uid, { email: e || null });
    return { ok: true, email: mascara(e) };
  },
  async esqueci(b, ip) {
    if (!MAIL_OK) throw new Erro(503, 'A recuperação por e-mail ainda não foi ligada. Fale com o Criador do jogo.');
    if (estourou('esq:' + ip, 6, 3600e3)) throw new Erro(429, 'Muitos pedidos. Tente daqui a uma hora.');
    const usuario = String(b.usuario || '').toLowerCase().trim();
    const msg = 'Se essa conta tiver e-mail, mandamos um código de 6 números pra ele. Olhe também o spam. Vale por 15 minutos.';
    const r = okUser(usuario) ? await DB.byUser(usuario) : null;
    if (!r || !r.email || r.banido) return { ok: true, msg };
    if (estourou('esqU:' + r.uid, 3, 3600e3)) return { ok: true, msg };
    const cod = String(crypto.randomInt(100000, 1000000));
    await DB.update(r.uid, { reset: { h: hashCodigo(r.uid, cod), exp: Date.now() + 15 * 60e3, n: 0 } });
    try {
      await mandaEmail(
        r.email,
        'WorldBullet · código para trocar a senha',
        '<div style="font-family:Arial,sans-serif;font-size:16px"><h2 style="color:#e0a800">WorldBullet</h2><p>Olá, <b>' +
          String(r.nome).replace(/[<>&]/g, '') +
          '</b>! Seu código para criar uma senha nova é:</p><p style="font-size:32px;font-weight:bold;letter-spacing:6px">' +
          cod +
          '</p><p>Ele vale por 15 minutos. Se não foi você que pediu, é só ignorar este e-mail.</p></div>',
      );
    } catch (e) {
      console.warn('e-mail não foi:', e.message);
      throw new Erro(503, 'Não deu para mandar o e-mail agora. Tente de novo mais tarde.');
    }
    return { ok: true, msg };
  },
  async redefinir(b, ip) {
    if (estourou('red:' + ip, 20, 3600e3)) throw new Erro(429, 'Muitas tentativas. Tente mais tarde.');
    const usuario = String(b.usuario || '').toLowerCase().trim(),
      cod = String(b.codigo || '').replace(/\D/g, ''),
      nova = String(b.nova || '');
    if (nova.length < 6 || nova.length > 64) throw new Erro(400, 'A senha nova precisa ter de 6 a 64 caracteres.');
    const r = okUser(usuario) ? await DB.byUser(usuario) : null,
      R = r && r.reset;
    if (!R || !R.h) throw new Erro(400, 'Peça um código primeiro.');
    if (R.exp < Date.now() || R.n >= 5) {
      await DB.update(r.uid, { reset: null });
      throw new Erro(400, 'Esse código venceu. Peça outro.');
    }
    const a = Buffer.from(hashCodigo(r.uid, cod)),
      c = Buffer.from(R.h);
    if (a.length !== c.length || !crypto.timingSafeEqual(a, c)) {
      await DB.update(r.uid, { reset: Object.assign({}, R, { n: (R.n | 0) + 1 }) });
      throw new Erro(401, 'Código errado.');
    }
    const r2 = await DB.update(r.uid, { senha: await hashSenha(nova), reset: null, visto: agora() });
    RL.delete('falha:' + usuario);
    return { token: fazToken(r.uid), conta: await minha(r2 || r, true) };
  },
  // ---------- eventos ----------
  async eventos(b) {
    const A = await eventosAtivos();
    let meus = [];
    const uid = leToken(b.token);
    if (uid) {
      const r = await contaDoToken(uid).catch(() => null);
      if (r) meus = arr(r.eventos);
    }
    return { eventos: A.map(evPub), pegos: meus, agora: Date.now() };
  },
  async evento_resgatar(b) {
    const r = await quem(b),
      e = (await eventosAtivos()).find((x) => x.id === b.id && x.tipo === 'presente');
    if (!e) throw new Erro(404, 'Esse evento acabou.');
    if (arr(r.eventos).includes(e.id)) return { ok: true, ja: true };
    await DB.update(r.uid, { eventos: arr(r.eventos).concat(e.id).slice(-100) });
    const item = e.item ? { tipo: 'item', cat: e.item.cat, item: e.item.id, nome: e.item.nome } : { tipo: 'moedas', n: e.n | 0 };
    await daPresente(await DB.get(r.uid), Object.assign(item, { msg: 'Presente do evento ' + e.nome + ' 🎉' }), 'Evento');
    return { ok: true };
  },
  // ---------- guildas ----------
  async guilda(b) {
    const r = await quem(b),
      ac = String(b.acao || '');
    if (estourou('gd:' + r.uid, 60, 60e3)) throw new Erro(429, 'Calma, muitas ações seguidas.');
    await guildasCache();
    const minhaG = async () => {
      const g = r.guilda ? await XB.g.get(r.guilda) : null;
      return g && cargoDe(g, r.uid) ? g : null;
    };
    const lista = async () => {
      await carregaRank();
      return GC.list.map((g) => ({ id: g.id, nome: g.nome, tag: g.tag, emblema: g.emblema, descr: g.descr || '', aberta: !!g.aberta, n: arr(g.membros).length, pontos: gPontos(g) }));
    };
    if (ac === 'minha') {
      const g = await minhaG();
      return { guilda: g ? await gDetalhe(g, r.uid) : null };
    }
    if (ac === 'ver') {
      const g = await XB.g.get(String(b.id || ''));
      if (!g) throw new Erro(404, 'Guilda não encontrada.');
      return { guilda: await gDetalhe(g, r.uid) };
    }
    if (ac === 'buscar') {
      const q = chaveG(limpaTxt(b.q, 16));
      const L = (await lista()).filter((g) => !q || chaveG(g.nome).includes(q) || g.tag.toLowerCase() === q);
      return { guildas: L.sort((a, c) => c.pontos - a.pontos || c.n - a.n).slice(0, 30) };
    }
    if (ac === 'ranking') return { guildas: (await lista()).sort((a, c) => c.pontos - a.pontos || c.n - a.n).slice(0, 50) };
    if (ac === 'criar') {
      if (await minhaG()) throw new Erro(400, 'Você já está numa guilda. Saia dela primeiro.');
      const nome = limpaTxt(b.nome, 16).replace(/\s+/g, ' '),
        tag = String(b.tag || '')
          .toUpperCase()
          .replace(/[^A-Z0-9]/g, '');
      if (nome.length < 3) throw new Erro(400, 'O nome da guilda precisa de 3 a 16 letras.');
      if (tag.length < 2 || tag.length > 4) throw new Erro(400, 'A sigla precisa de 2 a 4 letras ou números.');
      const g = {
        id: 'g' + crypto.randomBytes(4).toString('hex'),
        nome,
        chave: chaveG(nome),
        tag,
        emblema: EMBLEMAS.includes(b.emblema) ? b.emblema : EMBLEMAS[0],
        descr: limpaTxt(b.descr, 80),
        dono: r.uid,
        membros: [{ u: r.uid, c: 'lider', t: Date.now() }],
        pedidos: [],
        chat: [{ s: 1, m: r.nome + ' criou a guilda 🎉', t: Date.now() }],
        aberta: b.aberta !== false,
        criado: agora(),
      };
      if (await XB.g.byChave(g.chave)) throw new Erro(409, 'Já existe uma guilda com esse nome.');
      const ok = await XB.g.insert(g);
      if (!ok) throw new Erro(409, 'Já existe uma guilda com esse nome.');
      await DB.update(r.uid, { guilda: g.id });
      GC.t = 0;
      return { guilda: await gDetalhe(g, r.uid) };
    }
    if (ac === 'entrar') {
      if (await minhaG()) throw new Erro(400, 'Você já está numa guilda.');
      const g = await XB.g.get(String(b.id || ''));
      if (!g) throw new Erro(404, 'Guilda não encontrada.');
      if (arr(g.membros).length >= GMAX) throw new Erro(400, 'Essa guilda está cheia (' + GMAX + ' membros).');
      if (!g.aberta) {
        if (!arr(g.pedidos).includes(r.uid)) await XB.g.update(g.id, { pedidos: com(g.pedidos, r.uid, 50) });
        gAvisa(g, { t: 'ct', gped: { nome: r.nome } }, ['lider', 'vice']);
        return { msg: 'Pedido enviado! O líder da ' + g.nome + ' precisa aceitar.' };
      }
      const ng = await XB.g.update(g.id, {
        membros: arr(g.membros).concat({ u: r.uid, c: 'membro', t: Date.now() }),
        pedidos: sem(g.pedidos, r.uid),
        chat: arr(g.chat).concat({ s: 1, m: r.nome + ' entrou na guilda 👋', t: Date.now() }).slice(-40),
      });
      await DB.update(r.uid, { guilda: g.id });
      GC.t = 0;
      gAvisa(ng, { t: 'gch' });
      return { msg: 'Bem-vindo à ' + g.nome + '!', guilda: await gDetalhe(ng, r.uid) };
    }
    // daqui pra baixo: só quem está numa guilda
    const g = await minhaG();
    if (!g) throw new Erro(400, 'Você não está numa guilda.');
    const eu = cargoDe(g, r.uid),
      chefe = eu === 'lider' || eu === 'vice';
    if (ac === 'sair') {
      await gTira(g, r.uid, r.nome + ' saiu da guilda');
      return { msg: 'Você saiu da ' + g.nome + '.', guilda: null };
    }
    if (ac === 'chat') {
      const m = limpaTxt(b.m, 140);
      if (!m) throw new Erro(400, 'Escreva a mensagem.');
      if (estourou('gc:' + r.uid, 20, 60e3)) throw new Erro(429, 'Calma, muitas mensagens seguidas.');
      const msg = { u: r.uid, n: r.nome, vf: r.vf | 0, m, t: Date.now() };
      await XB.g.update(g.id, { chat: arr(g.chat).concat(msg).slice(-40) });
      gAvisa(g, { t: 'gch', g: g.id, msg });
      return { ok: true };
    }
    if (ac === 'aceitar' || ac === 'recusar') {
      if (!chefe) throw new Erro(403, 'Só o líder ou o vice podem fazer isso.');
      const uid = String(b.uid || '');
      if (!arr(g.pedidos).includes(uid)) throw new Erro(400, 'Esse pedido não existe mais.');
      if (ac === 'recusar') {
        await XB.g.update(g.id, { pedidos: sem(g.pedidos, uid) });
        return { guilda: await gDetalhe(await XB.g.get(g.id), r.uid) };
      }
      const a = await DB.get(uid);
      if (!a) throw new Erro(404, 'Esse jogador não existe mais.');
      if (a.guilda && a.guilda !== g.id) {
        await XB.g.update(g.id, { pedidos: sem(g.pedidos, uid) });
        throw new Erro(400, a.nome + ' já entrou em outra guilda.');
      }
      if (arr(g.membros).length >= GMAX) throw new Erro(400, 'A guilda está cheia.');
      const ng = await XB.g.update(g.id, {
        membros: arr(g.membros).concat({ u: uid, c: 'membro', t: Date.now() }),
        pedidos: sem(g.pedidos, uid),
        chat: arr(g.chat).concat({ s: 1, m: a.nome + ' entrou na guilda 👋', t: Date.now() }).slice(-40),
      });
      await DB.update(uid, { guilda: g.id });
      avisa(uid, { t: 'ct', gok: { nome: g.nome } });
      gAvisa(ng, { t: 'gch' });
      GC.t = 0;
      return { guilda: await gDetalhe(ng, r.uid) };
    }
    if (ac === 'expulsar') {
      const uid = String(b.uid || ''),
        c = cargoDe(g, uid);
      if (!c || uid === r.uid) throw new Erro(400, 'Esse jogador não está na guilda.');
      if (!(eu === 'lider' || (eu === 'vice' && c === 'membro'))) throw new Erro(403, 'Você não pode expulsar esse jogador.');
      const a = await DB.get(uid);
      await gTira(g, uid, (a ? a.nome : 'Alguém') + ' foi removido da guilda');
      avisa(uid, { t: 'ct', gfora: { nome: g.nome } });
      return { guilda: await gDetalhe(await XB.g.get(g.id), r.uid) };
    }
    if (ac === 'cargo') {
      if (eu !== 'lider') throw new Erro(403, 'Só o líder pode mudar cargos.');
      const uid = String(b.uid || ''),
        c = String(b.c || '');
      if (!cargoDe(g, uid) || uid === r.uid || !['vice', 'membro', 'lider'].includes(c)) throw new Erro(400, 'Cargo inválido.');
      const M = arr(g.membros).map((m) => Object.assign({}, m));
      for (const m of M) if (m.u === uid) m.c = c;
      if (c === 'lider') for (const m of M) if (m.u === r.uid) m.c = 'vice'; // passou a liderança
      const ng = await XB.g.update(g.id, { membros: M });
      gAvisa(ng, { t: 'gch' });
      return { guilda: await gDetalhe(ng, r.uid) };
    }
    if (ac === 'editar') {
      if (!chefe) throw new Erro(403, 'Só o líder ou o vice podem editar.');
      const p = {};
      if (b.descr !== undefined) p.descr = limpaTxt(b.descr, 80);
      if (b.aberta !== undefined) p.aberta = !!b.aberta;
      if (EMBLEMAS.includes(b.emblema)) p.emblema = b.emblema;
      const ng = await XB.g.update(g.id, p);
      GC.t = 0;
      return { guilda: await gDetalhe(ng, r.uid) };
    }
    throw new Erro(400, 'Ação desconhecida.');
  },
  // excluir a conta de vez (pede a senha). Também sai da lista de amigos de todo mundo.
  async excluir(b, ip) {
    const r = await quem(b);
    if (estourou('ex:' + ip, 10, 3600e3)) throw new Erro(429, 'Muitas tentativas. Tente mais tarde.');
    if (!(await confereSenha(String(b.senha || '').slice(0, 64), r.senha))) throw new Erro(401, 'Senha errada.');
    if (r.admin) throw new Erro(400, 'A conta do Criador não pode ser excluída pelo jogo (tire o ADMIN no Render antes).');
    const outros = new Set(arr(r.amigos).concat(arr(r.pedidos), arr(r.enviados)));
    for (const f of outros) {
      const a = await DB.get(f).catch(() => null);
      if (a) await DB.update(f, { amigos: sem(a.amigos, r.uid), pedidos: sem(a.pedidos, r.uid), enviados: sem(a.enviados, r.uid) });
    }
    if (r.guilda) {
      const g = await XB.g.get(r.guilda).catch(() => null);
      if (g) await gTira(g, r.uid, r.nome + ' saiu da guilda').catch(() => {});
    }
    await DB.remove(r.uid);
    const S = porConta.get(r.uid);
    if (S) for (const c of S) (c.uid = null), (c.vf = 0);
    porConta.delete(r.uid);
    RANK.t = 0;
    console.log('conta excluída', r.uid);
    return { ok: true };
  },
  async admin(b) {
    const r = await quem(b);
    if (!r.admin) throw new Erro(403, 'Só o Criador pode usar isso.');
    if (b.acao === 'denuncias') {
      const L = await XB.d.list('aberta'),
        P = await DB.many([...new Set(L.flatMap((d) => [d.alvo, d.autor]))]),
        by = {};
      for (const p of P) by[p.uid] = p;
      return {
        denuncias: L.map((d) => ({ id: d.id, alvo: { uid: d.alvo, nome: (by[d.alvo] || {}).nome || '(conta excluída)' }, autor: { uid: d.autor, nome: (by[d.autor] || {}).nome || '?' }, motivo: MOTIVOS[d.motivo] || d.motivo, detalhe: d.detalhe || '', criado: d.criado })),
      };
    }
    if (b.acao === 'denuncia') {
      await XB.d.update(b.id, { status: b.st === 'ignorada' ? 'ignorada' : 'resolvida' });
      return { ok: true };
    }
    if (b.acao === 'vendas') {
      const L = await XB.p.lista(null),
        P = await DB.many([...new Set(L.map((q) => q.uid))]),
        by = {};
      for (const p of P) by[p.uid] = p.nome;
      const tot = L.reduce((t, q) => t + +q.valor, 0);
      return { vendas: L.map((q) => ({ id: q.id, nome: by[q.uid] || '?', uid: q.uid, diamantes: q.diamantes, valor: q.valor, pago: q.pago || q.criado, metodo: q.metodo })), total: Math.round(tot * 100) / 100, loja: MP_OK };
    }
    if (b.acao === 'eventos') return { eventos: (await eventosAtivos(true)).map(evPub), tipos: ['moedas2x', 'xp2x', 'presente'] };
    if (b.acao === 'evento_criar') {
      const tipo = String(b.tipo || '');
      if (!['moedas2x', 'xp2x', 'presente'].includes(tipo)) throw new Erro(400, 'Tipo de evento inválido.');
      const h = Math.max(1, Math.min(720, +b.horas || 0)),
        nome = limpaTxt(b.nome, 40) || (tipo === 'moedas2x' ? 'Moedas em dobro' : tipo === 'xp2x' ? 'XP em dobro' : 'Presente');
      const e = { id: novoId(), tipo, nome, ini: Date.now(), fim: Date.now() + h * 3600e3 };
      if (tipo === 'presente') {
        if (b.cat && ['g', 'p', 'ch', 'e', 'a', 'b'].includes(b.cat) && b.id) e.item = { cat: b.cat, id: limpaTxt(b.id, 40), nome: limpaTxt(b.nomeItem, 40) || 'Item' };
        else e.n = Math.max(1, Math.min(1e5, Math.floor(+b.n || 0)));
      }
      await eventosAtivos(true);
      const L = EV.list.filter((x) => x && x.fim > Date.now()).concat(e);
      await XB.c.set('eventos', L);
      EV.t = 0;
      for (const S of porConta.values()) for (const c of S) send(c, { t: 'ev' }); // todo mundo online vê na hora
      return { ok: true, eventos: (await eventosAtivos(true)).map(evPub) };
    }
    if (b.acao === 'evento_fim') {
      await eventosAtivos(true);
      const L = EV.list.map((x) => (x && x.id === b.id ? Object.assign({}, x, { fim: Date.now() - 1 }) : x)).filter((x) => x && x.fim > Date.now() - 864e5);
      await XB.c.set('eventos', L);
      EV.t = 0;
      for (const S of porConta.values()) for (const c of S) send(c, { t: 'ev' });
      return { ok: true, eventos: (await eventosAtivos(true)).map(evPub) };
    }
    // v134: quantos estão online e aviso de atualização
    if (b.acao === 'status') {
      let emSala = 0;
      for (const c of clients.values()) if (c.code) emSala++;
      return { online: clients.size, contas: porConta.size, salas: roomCount(), emSala, v: GAME_VER, rein: REIN.ate > Date.now() ? REIN.ate : 0 };
    }
    if (b.acao === 'reinicio' || b.acao === 'reinicio_cancelar') {
      REIN.ate = b.acao === 'reinicio' ? Date.now() + Math.max(1, Math.min(10, Math.floor(+b.min || 3))) * 60e3 : 0;
      for (const c of clients.values()) send(c, { t: 'rein', ate: REIN.ate });
      return { ok: true, rein: REIN.ate };
    }
    if (b.acao === 'correio') return { lista: (await correioGlobal(true)).map(cgPub) };
    if (b.acao === 'correio_todos') {
      const msg = limpaTxt(b.msg, 200),
        dias = Math.max(1, Math.min(60, Math.floor(+b.dias || 7)));
      let item;
      if (b.tipo === 'moedas') {
        const n = Math.floor(+b.n);
        if (!(n >= 1 && n <= 1e6)) throw new Erro(400, 'Quantidade de moedas inválida.');
        item = { tipo: 'moedas', n, msg };
      } else if (b.tipo === 'item') {
        const cat = String(b.cat || ''),
          id = limpaTxt(b.id, 40);
        if (!['g', 'p', 'ch', 'e', 'a', 'b'].includes(cat) || !id) throw new Erro(400, 'Item inválido.');
        item = { tipo: 'item', cat, item: id, nome: limpaTxt(b.nome, 40), msg };
      } else {
        if (!msg) throw new Erro(400, 'Escreva a mensagem.');
        item = { tipo: 'msg', msg };
      }
      const x = { id: novoId(), ini: Date.now(), fim: Date.now() + dias * 864e5, de: r.nome, item };
      await correioGlobal(true);
      await XB.c.set('correio', CG.list.filter((y) => y && y.fim > Date.now()).concat(x));
      CG.t = 0;
      for (const S of porConta.values()) for (const c of S) send(c, { t: 'ct' }); // quem está online recebe na hora
      return { ok: true, lista: (await correioGlobal(true)).map(cgPub) };
    }
    if (b.acao === 'correio_fim') {
      await correioGlobal(true);
      await XB.c.set('correio', CG.list.filter((y) => y && y.id !== b.id && y.fim > Date.now()));
      CG.t = 0;
      return { ok: true, lista: (await correioGlobal(true)).map(cgPub) };
    }
    const a = await alvo(String(b.uid || '').replace(/\D/g, '')),
      ac = String(b.acao || ''),
      de = r.nome,
      msg = limpaTxt(b.msg, 200);
    if (ac === 'ver')
      return {
        jogador: Object.assign(publico(a), { usuario: a.usuario, admin: !!a.admin, banido: !!a.banido, ban: banInfo(a), acesso: arr(a.acesso), criado: a.criado, visto: a.visto, sus: n0(obj(a.stats)._sus), diamantes: n0(a.diamantes) }),
      };
    if (ac === 'verificar') {
      if (a.admin) throw new Erro(400, 'Essa conta é de Criador.');
      const vf = b.on ? 1 : 0;
      await DB.update(a.uid, { vf });
      const S = porConta.get(a.uid);
      if (S)
        for (const c of S) {
          c.vf = vf;
          if (c.pr) onPresence(c, c.pr); // todo mundo vê o selo na hora
        }
      if (vf) await daPresente(a, { tipo: 'vf', msg: msg || 'Você ganhou o selo de conta verificada! 💙' }, de);
      else avisa(a.uid, { t: 'ct' });
    } else if (ac === 'moedas') {
      const n = Math.floor(+b.n);
      if (!(n >= 1 && n <= 1e6)) throw new Erro(400, 'Quantidade de moedas inválida.');
      await daPresente(a, { tipo: 'moedas', n, msg }, de);
    } else if (ac === 'item') {
      const cat = String(b.cat || ''),
        id = limpaTxt(b.id, 40);
      if (!['g', 'p', 'ch', 'e', 'a', 'b'].includes(cat) || !id) throw new Erro(400, 'Item inválido.'); // v132: emotes também (roupas saíram)
      await daPresente(a, { tipo: 'item', cat, item: id, nome: limpaTxt(b.nome, 40), msg }, de);
    } else if (ac === 'acesso') {
      const tag = String(b.tag || '').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 20);
      if (!tag) throw new Erro(400, 'Nome do acesso inválido.');
      await DB.update(a.uid, { acesso: b.on ? com(a.acesso, tag, 30) : sem(a.acesso, tag) });
      if (b.on) await daPresente(await DB.get(a.uid), { tipo: 'acesso', tag, msg: msg || 'Você ganhou acesso antecipado! 🔑' }, de);
      else avisa(a.uid, { t: 'ct' });
    } else if (ac === 'diamantes') {
      const n = Math.trunc(+b.n);
      if (!n || Math.abs(n) > 1e6) throw new Erro(400, 'Quantidade inválida.');
      await filaDm(a.uid, async () => {
        const r = await DB.get(a.uid);
        await DB.update(a.uid, { diamantes: Math.max(0, n0(r.diamantes) + n) });
      });
      avisa(a.uid, { t: 'ct', dm: n > 0 ? { n } : null });
    } else if (ac === 'msg') {
      if (!msg) throw new Erro(400, 'Escreva a mensagem.');
      await daPresente(a, { tipo: 'msg', msg }, de);
    } else if (ac === 'senha') {
      const nova = String(b.nova || '');
      if (nova.length < 6 || nova.length > 64) throw new Erro(400, 'A senha nova precisa ter de 6 a 64 caracteres.');
      await DB.update(a.uid, { senha: await hashSenha(nova) });
    } else if (ac === 'banir') {
      if (a.admin) throw new Erro(400, 'Não dá para banir um Criador.');
      if (b.on) {
        // v139: suspensão com tempo (horas; 0 = permanente) e motivo
        const motivo = BAN_MOTIVOS[b.motivo] ? b.motivo : '';
        if (!motivo) throw new Erro(400, 'Escolha o motivo da suspensão.');
        const horas = Math.max(0, Math.min(24 * 365, +b.horas || 0)),
          ate = horas ? Date.now() + horas * 3600e3 : 0;
        await DB.update(a.uid, { banido: true, eventos: semBan(a).concat('ban|' + ate + '|' + motivo) });
        // quem estiver jogando recebe o aviso NA HORA e cai para a tela de entrar
        const S = porConta.get(a.uid);
        if (S)
          for (const c of [...S]) {
            send(c, { t: 'ban', ate, motivo });
            setTimeout(() => closeClient(c), 400);
          }
      } else await DB.update(a.uid, { banido: false, eventos: semBan(a) });
    } else throw new Erro(400, 'Ação desconhecida.');
    return { ok: true, jogador: publico(await DB.get(a.uid)) };
  },
};
// ---- Política de privacidade e regras (link no jogo e para a Play Store) ----
const CONTATO = envLimpo('CONTATO'); // seu e-mail ou @ para contato (variável no Render)
function paginaPrivacidade() {
  const esc = (t) => String(t).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
  const ct = CONTATO ? esc(CONTATO) : 'o Criador do jogo (pelo próprio jogo)';
  return `<!DOCTYPE html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WorldBullet · Privacidade e regras</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:760px;margin:0 auto;padding:24px 18px 60px;background:#120c26;color:#eee}
h1{color:#ffc928;font-style:italic}h2{color:#ffc928;margin-top:28px;font-size:19px}a{color:#ffc928}li{margin:4px 0}small{color:#aaa}</style></head><body>
<h1>WorldBullet — Privacidade e regras</h1><small>Última atualização: novembro de 2026</small>
<h2>O que guardamos</h2><p>Só quando você cria uma conta:</p><ul>
<li><b>Nome de usuário e apelido</b> que você escolheu (o apelido e o ID aparecem para outros jogadores, no ranking e na lista de amigos).</li>
<li><b>Senha</b> — guardada embaralhada (hash scrypt). Ninguém consegue ler, nem o Criador.</li>
<li><b>Progresso do jogo</b>: moedas, itens, personagens, patentes, partidas, abates e vitórias.</li>
<li><b>Amigos, pedidos de amizade e presentes</b> recebidos.</li>
<li>Datas de criação da conta e do último acesso.</li>
<li><b>E-mail (opcional)</b> — só se você colocar, e só é usado para mandar o código de trocar a senha.</li>
<li><b>Compras de diamantes</b>: número do pedido, pacote, valor e data. O pagamento é feito no <b>Mercado Pago</b>: dados de cartão e Pix ficam lá, nunca passam pelo jogo.</li>
<li><b>Guilda</b> e mensagens do chat da guilda (os membros veem). <b>Denúncias</b> que você fizer (só o Criador vê).</li></ul>
<p>Não pedimos telefone, nome verdadeiro, localização nem fotos (o e-mail é opcional). Não tem anúncios nem rastreamento de terceiros. Sem conta, o progresso fica só no seu aparelho.</p>
<h2>Para que usamos</h2><ul><li>Salvar seu progresso e deixar você entrar em qualquer aparelho.</li><li>Mostrar o ranking, os amigos online e os convites para sala.</li><li>Evitar trapaça (o servidor confere se os números do jogo são possíveis).</li></ul>
<p>Seus dados <b>não são vendidos nem passados</b> para ninguém. Eles ficam guardados nos serviços que hospedam o jogo (Render, servidor do jogo, e Supabase, banco de dados).</p>
<h2>Menores de idade</h2><p>Se você tem menos de 12 anos, crie a conta junto com seu pai, mãe ou responsável. Nunca use seu nome completo, endereço ou telefone como nome de usuário ou apelido.</p>
<h2>Excluir sua conta</h2><p>No jogo: toque no seu nome lá em cima → <b>Perfil</b> → <b>Excluir conta</b> → digite sua senha. A conta, o progresso e as amizades são apagados <b>na hora e para sempre</b>. Se não conseguir entrar, fale com ${ct} informando seu nome de usuário.</p>
<h2>Seus direitos (LGPD)</h2><p>Você pode pedir para ver, corrigir ou apagar seus dados a qualquer momento: fale com ${ct}.</p>
<h2>Compras</h2><p>Diamantes compram só itens escolhidos (você vê o item antes). Não existe caixa ou roleta paga com dinheiro. Menores de 18 anos só compram com autorização do pai, mãe ou responsável. Problema com uma compra? Fale com ${ct} informando o número do pedido.</p>
<h2>Regras do jogo</h2><ul><li>Proibido usar trapaça, programas ou alterar o jogo para ganhar moedas, pontos ou vantagem.</li><li>Respeite os outros jogadores: nada de apelidos ofensivos, preconceito ou ameaças.</li><li>Não compartilhe sua senha. Não tente entrar na conta dos outros.</li><li>Quem quebrar as regras pode ter os pontos cortados ou a conta <b>banida</b>.</li></ul>
<h2>Contato</h2><p>${ct}</p></body></html>`;
}
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'POST, OPTIONS' };
function responde(res, st, o) {
  res.writeHead(st, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, CORS));
  res.end(JSON.stringify(o));
}
function apiHttp(req, res, url) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    return res.end();
  }
  const nome = url.slice(5),
    fn = Object.prototype.hasOwnProperty.call(API, nome) ? API[nome] : null;
  if (!fn || req.method !== 'POST') return responde(res, 404, { erro: 'Não encontrado' });
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
  if (estourou('ip:' + ip, 240, 60e3)) return responde(res, 429, { erro: 'Muitos pedidos. Espere um pouco.' });
  let n = 0;
  const parts = [];
  req.on('data', (d) => {
    n += d.length;
    if (n > 200 * 1024) {
      responde(res, 413, { erro: 'Grande demais.' });
      req.destroy();
    } else parts.push(d);
  });
  req.on('end', async () => {
    if (res.writableEnded) return;
    let b;
    try {
      b = JSON.parse(Buffer.concat(parts).toString('utf8') || '{}');
      if (!b || typeof b !== 'object') throw 0;
    } catch (e) {
      return responde(res, 400, { erro: 'Pedido inválido.' });
    }
    try {
      responde(res, 200, await fn(b, ip, req));
    } catch (e) {
      if (e instanceof Erro) return responde(res, e.st, Object.assign({ erro: e.message }, e.extra || {}));
      console.warn('api', nome, e.message);
      DIAG = { ok: false, msg: explicaErro(e) };
      console.warn('Contas (banco):', DIAG.msg);
      responde(res, 503, { erro: 'O banco de contas não respondeu. (Dono do jogo: abra /status para ver o motivo.)' });
    }
  });
}
// conexão do jogo diz de qual conta é (o selo e o "online" vêm daqui, não do jogo)
async function wsAuth(c, tk) {
  const sai = () => {
    if (c.uid && porConta.has(c.uid)) porConta.get(c.uid).delete(c);
    c.uid = null;
    c.vf = 0;
  };
  if (!tk) return sai();
  const uid = leToken(tk);
  let r = null;
  try {
    r = uid ? await contaDoToken(uid) : null;
  } catch (e) {}
  let B = null;
  if (r && r.banido)
    try {
      r = await banChk(r); // suspensão acabou? libera
    } catch (e) {
      B = (e.extra && e.extra.ban) || { ate: 0, motivo: '' };
    }
  if (!c.alive) return;
  if (!r || r.banido || B) {
    sai();
    return send(c, B ? { t: 'auth', ok: false, ban: B } : { t: 'auth', ok: false });
  }
  sai();
  c.uid = r.uid;
  c.vf = r.vf | 0;
  if (!porConta.has(r.uid)) porConta.set(r.uid, new Set());
  porConta.get(r.uid).add(c);
  send(c, { t: 'auth', ok: true, vf: c.vf });
  if (c.pr) onPresence(c, c.pr);
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
  send(c, { t: 'hi', id: c.id, n: clients.size, v: GAME_VER, rein: REIN.ate > Date.now() ? REIN.ate : 0 });
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
