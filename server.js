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
  if (url.startsWith('/api/')) return apiHttp(req, res, url);
  if (url === '/privacidade' || url === '/privacidade/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(paginaPrivacidade());
  }
  if (url === '/status') {
    return testaBanco().then((d) => {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ ok: true, jogadores: clients.size, salas: roomCount(), contas: DB.nome, banco: d.msg, enderecoBanco: SB_URL || null, admin: ADMIN || null, enviadoMB: +(sentBytes / 1e6).toFixed(1) }, null, 1));
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
  const h = { apikey: SB_KEY, 'Content-Type': 'application/json', Accept: 'application/json' };
  if (/^eyJ/.test(SB_KEY)) h.Authorization = 'Bearer ' + SB_KEY; // chave antiga (service_role)
  if (prefer) h.Prefer = prefer;
  const r = await fetch(SB_URL + '/rest/v1/contas' + query, { method, headers: h, body: body == null ? undefined : JSON.stringify(body) });
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
    async ranking() {
      return sbReq('GET', '?select=uid,nome,vf,banido,rp:stats->rp,st4:stats->st4,wins:stats->wins,kills:stats->kills&limit=5000');
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
      save();
      return cp(r);
    },
    async remove(uid) {
      M.delete(uid);
      save();
    },
    async ranking() {
      return [...M.values()].map((r) => ({ uid: r.uid, nome: r.nome, vf: r.vf, banido: r.banido, rp: (r.stats || {}).rp, st4: (r.stats || {}).st4, wins: (r.stats || {}).wins, kills: (r.stats || {}).kills }));
    },
  };
}
const DB = SB_URL && SB_KEY ? bancoSupabase() : bancoLocal();
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
  } catch (e) {
    DIAG = { ok: false, msg: explicaErro(e) };
  }
  console.log('Contas (banco):', DIAG.msg, '| URL usada:', SB_URL);
  return DIAG;
}
testaBanco();

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

const okUid = (s) => typeof s === 'string' && /^[1-9]\d{8}$/.test(s);
const okUser = (s) => typeof s === 'string' && /^[a-z0-9_.]{3,16}$/.test(s);
const limpaTxt = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, n);
const limpaNome = (s) => limpaTxt(s, 14) || 'Jogador';
const objOk = (o, max) => !!o && typeof o === 'object' && !Array.isArray(o) && JSON.stringify(o).length <= max;
const arr = (a) => (Array.isArray(a) ? a : []);
const novoUid = () => String(100000000 + crypto.randomInt(900000000));
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
    if (c.pr && (c.pr.st === 'play' || c.pr.st === 'dead')) st = 'play';
    if (c.code) sala = c.code;
  }
  return { st, sala };
}
const publico = (r) => ({ uid: r.uid, nome: r.nome, vf: r.vf | 0, on: !!onlineDe(r.uid) });
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
  if (full) {
    o.perfil = r.perfil || {};
    o.stats = r.stats || {};
  }
  return o;
}
class Erro extends Error {
  constructor(st, msg) {
    super(msg);
    this.st = st;
  }
}
async function quem(b) {
  const uid = leToken(b.token);
  if (!uid) throw new Erro(401, 'Sua sessão expirou. Entre de novo.');
  const r = await DB.get(uid);
  if (!r) throw new Erro(401, 'Conta não encontrada. Entre de novo.');
  if (r.banido) throw new Erro(403, 'Esta conta foi banida.');
  return r;
}
async function alvo(uid) {
  if (!okUid(uid)) throw new Erro(400, 'ID inválido (são 9 números).');
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
  corta(n, 'coins', o.coins, 6000 + dm * 2500 + presente, 'moedas');
  PRESENTE_MOEDAS.delete(uid);
  if (n0(n.wins) > n0(n.matches)) n.wins = n0(n.matches);
  n._sus = n0(o._sus) + (sus.length ? 1 : 0); // quantas vezes já foi cortado (o Criador vê)
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
const RANK = { t: 0, br: null, cs: null, p: null }; // ranking guardado por 45 s (não pesa no banco)
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
    let uid = okUid(b.uid) && !(await DB.get(b.uid)) ? b.uid : null; // mantém o ID que ele já tinha
    for (let i = 0; i < 6; i++) {
      if (!uid) uid = novoUid();
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
    if (r.banido) throw new Erro(403, 'Esta conta foi banida.');
    const patch = { visto: agora() };
    if (ADMIN && usuario === ADMIN && !r.admin) Object.assign(patch, { admin: true, vf: 2 });
    r = (await DB.update(r.uid, patch)) || r;
    return { token: fazToken(r.uid), conta: await minha(r, true) };
  },
  async eu(b) {
    const r = await quem(b);
    return { conta: await minha(r, !!b.full) };
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
    if (Date.now() - RANK.t > 45e3 || !RANK.br) {
      if (!RANK.p)
        RANK.p = DB.ranking()
          .then((rows) => {
            const L = (rows || [])
              .filter((r) => r && !r.banido)
              .map((r) => ({ uid: r.uid, nome: r.nome, vf: r.vf | 0, rp: Math.max(0, +r.rp || 0), st4: Math.max(0, +r.st4 || 0), wins: +r.wins || 0, kills: +r.kills || 0 }));
            RANK.br = L.filter((r) => r.rp > 0).sort((a, b) => b.rp - a.rp || b.wins - a.wins);
            RANK.cs = L.filter((r) => r.st4 > 0).sort((a, b) => b.st4 - a.st4 || b.wins - a.wins);
            RANK.t = Date.now();
          })
          .finally(() => (RANK.p = null));
      await RANK.p;
    }
    const uid = leToken(b.token),
      top = (A) => A.slice(0, 100).map((r) => Object.assign({}, r, { on: !!onlineDe(r.uid) })),
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
    const a = await alvo(String(b.uid || '').replace(/\D/g, '')),
      ac = String(b.acao || ''),
      de = r.nome,
      msg = limpaTxt(b.msg, 200);
    if (ac === 'ver')
      return {
        jogador: Object.assign(publico(a), { usuario: a.usuario, admin: !!a.admin, banido: !!a.banido, acesso: arr(a.acesso), criado: a.criado, visto: a.visto, sus: n0(obj(a.stats)._sus) }),
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
      if (!['o', 'g', 'p', 'ch'].includes(cat) || !id) throw new Erro(400, 'Item inválido.');
      await daPresente(a, { tipo: 'item', cat, item: id, nome: limpaTxt(b.nome, 40), msg }, de);
    } else if (ac === 'acesso') {
      const tag = String(b.tag || '').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 20);
      if (!tag) throw new Erro(400, 'Nome do acesso inválido.');
      await DB.update(a.uid, { acesso: b.on ? com(a.acesso, tag, 30) : sem(a.acesso, tag) });
      if (b.on) await daPresente(await DB.get(a.uid), { tipo: 'acesso', tag, msg: msg || 'Você ganhou acesso antecipado! 🔑' }, de);
      else avisa(a.uid, { t: 'ct' });
    } else if (ac === 'msg') {
      if (!msg) throw new Erro(400, 'Escreva a mensagem.');
      await daPresente(a, { tipo: 'msg', msg }, de);
    } else if (ac === 'senha') {
      const nova = String(b.nova || '');
      if (nova.length < 6 || nova.length > 64) throw new Erro(400, 'A senha nova precisa ter de 6 a 64 caracteres.');
      await DB.update(a.uid, { senha: await hashSenha(nova) });
    } else if (ac === 'banir') {
      if (a.admin) throw new Erro(400, 'Não dá para banir um Criador.');
      await DB.update(a.uid, { banido: !!b.on });
      if (b.on) {
        const S = porConta.get(a.uid);
        if (S) for (const c of [...S]) closeClient(c);
      }
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
<li>Datas de criação da conta e do último acesso.</li></ul>
<p>Não pedimos e-mail, telefone, nome verdadeiro, localização nem fotos. Não tem anúncios nem rastreamento de terceiros. Sem conta, o progresso fica só no seu aparelho.</p>
<h2>Para que usamos</h2><ul><li>Salvar seu progresso e deixar você entrar em qualquer aparelho.</li><li>Mostrar o ranking, os amigos online e os convites para sala.</li><li>Evitar trapaça (o servidor confere se os números do jogo são possíveis).</li></ul>
<p>Seus dados <b>não são vendidos nem passados</b> para ninguém. Eles ficam guardados nos serviços que hospedam o jogo (Render, servidor do jogo, e Supabase, banco de dados).</p>
<h2>Menores de idade</h2><p>Se você tem menos de 12 anos, crie a conta junto com seu pai, mãe ou responsável. Nunca use seu nome completo, endereço ou telefone como nome de usuário ou apelido.</p>
<h2>Excluir sua conta</h2><p>No jogo: toque no seu nome lá em cima → <b>Perfil</b> → <b>Excluir conta</b> → digite sua senha. A conta, o progresso e as amizades são apagados <b>na hora e para sempre</b>. Se não conseguir entrar, fale com ${ct} informando seu nome de usuário.</p>
<h2>Seus direitos (LGPD)</h2><p>Você pode pedir para ver, corrigir ou apagar seus dados a qualquer momento: fale com ${ct}.</p>
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
      responde(res, 200, await fn(b, ip));
    } catch (e) {
      if (e instanceof Erro) return responde(res, e.st, { erro: e.message });
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
    r = uid ? await DB.get(uid) : null;
  } catch (e) {}
  if (!c.alive) return;
  if (!r || r.banido) {
    sai();
    return send(c, { t: 'auth', ok: false });
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
