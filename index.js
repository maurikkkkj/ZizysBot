import 'dotenv/config'
import { spawn, spawnSync } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'
import qrcode from 'qrcode-terminal'

// ---------- Configuração ----------
const PREFIX = '/'
const CREDITS = 'mauri'
const MAX_MESSAGE_AGE_SECONDS = 120

const OWNERS = (process.env.OWNER || '').split(',').map((s) => s.trim()).filter(Boolean)

const API_URL = 'https://api.ikyyxd.my.id/search/ytplayv4?q='
const MEDIA_DIR = 'media'
const MAX_DURATION = Math.min(Math.max(Number(process.env.MAX_DURACAO) || 600, 10), 3600)
const KEEP_AUDIOS = process.env.MANTER_AUDIOS === 'true'
const MAX_DOWNLOAD_BYTES = 60 * 1024 * 1024
const DOWNLOAD_PART_BYTES = 1024 * 1024
const DOWNLOAD_CONNECTIONS = 6
const RING_TIMEOUT_MS = 45_000
const RING_TIMEOUT_GROUP_MS = 60_000
const GROUP_START_FALLBACK_MS = 8_000
const HANGUP_TAIL_MS = 2_000
const MAX_ACTIVE_CALLS = Infinity
const MAX_QUEUE = 5
const MAX_SESSION_MS = 3 * 60 * 60_000
const PAUSE_TIMEOUT_MS = 10 * 60_000
const GROUP_CALL_TTL_MS = 6 * 60 * 60_000
const NORMALIZE_AUDIO = true
const DIRECT_PLAY = true
const DIRECT_END_MARGIN_MS = 4_000
const DEBUG_CALLS = false
const USER_AGENT = 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36'

const SAMPLE_RATE = 48_000
const FALLBACK_RATE = 16_000
const CHUNK_SAMPLES = SAMPLE_RATE / 2
const LOOKAHEAD_SECONDS = 1.5
const LOOKAHEAD_BUSY_SECONDS = 4
const PUMP_INTERVAL_MS = 200

class UserError extends Error {}

// ---------- Download e decodificação (roda na thread separada) ----------
function assertSafeUrl(raw) {
  let u
  try {
    u = new URL(raw)
  } catch {
    throw new UserError('A API devolveu um link de áudio inválido.')
  }
  const host = u.hostname
  if (u.protocol !== 'https:' || host === 'localhost' || host.endsWith('.local') || /^[\d.]+$/.test(host) || host.includes(':')) {
    throw new UserError('A API devolveu um link de áudio não permitido.')
  }
  return u.href
}

async function streamWhole(res, file) {
  let size = 0
  const limiter = new Transform({
    transform(chunk, _enc, cb) {
      size += chunk.length
      if (size > MAX_DOWNLOAD_BYTES) return cb(new UserError('O áudio é grande demais.'))
      cb(null, chunk)
    },
  })
  await pipeline(Readable.fromWeb(res.body), limiter, fs.createWriteStream(file))
  if (!size) throw new UserError('O download veio vazio.')
}

async function downloadParts(href, file, first, total) {
  const fh = await fs.promises.open(file, 'w')
  try {
    const head = Buffer.from(await first.arrayBuffer())
    await fh.write(head, 0, head.length, 0)

    const starts = []
    for (let s = head.length; s < total; s += DOWNLOAD_PART_BYTES) starts.push(s)

    let next = 0
    const lane = async () => {
      while (next < starts.length) {
        const start = starts[next++]
        const end = Math.min(start + DOWNLOAD_PART_BYTES, total) - 1
        const res = await fetch(href, {
          signal: AbortSignal.timeout(120_000),
          headers: { 'user-agent': USER_AGENT, range: `bytes=${start}-${end}` },
        })
        if (res.status !== 206) throw new UserError(`Não consegui baixar o áudio (erro ${res.status}).`)
        const buf = Buffer.from(await res.arrayBuffer())
        if (buf.length !== end - start + 1) throw new UserError('O download veio incompleto.')
        await fh.write(buf, 0, buf.length, start)
      }
    }
    await Promise.all(Array.from({ length: Math.min(DOWNLOAD_CONNECTIONS, starts.length) }, lane))
  } finally {
    await fh.close()
  }
}

function pickExt(href, contentType) {
  const fromUrl = path.extname(new URL(href).pathname).slice(1).toLowerCase()
  if (['mp3', 'm4a', 'aac', 'ogg', 'opus', 'wav', 'webm', 'flac', 'mp4'].includes(fromUrl)) return fromUrl
  const type = String(contentType || '').toLowerCase()
  if (type.includes('mpeg')) return 'mp3'
  if (type.includes('ogg')) return 'ogg'
  if (type.includes('webm')) return 'webm'
  if (type.includes('wav')) return 'wav'
  if (type.includes('aac')) return 'aac'
  return 'm4a'
}

async function downloadAudio(url) {
  fs.mkdirSync(MEDIA_DIR, { recursive: true })
  let file = null
  try {
    const href = assertSafeUrl(url)
    const first = await fetch(href, {
      signal: AbortSignal.timeout(180_000),
      headers: { 'user-agent': USER_AGENT, range: `bytes=0-${DOWNLOAD_PART_BYTES - 1}` },
    })
    if (!first.ok || !first.body) throw new UserError(`Não consegui baixar o áudio (erro ${first.status}).`)
    file = path.join(MEDIA_DIR, `${Date.now()}-${crypto.randomBytes(4).toString('hex')}.${pickExt(href, first.headers.get('content-type'))}`)

    const total = first.status === 206 ? Number(/\/(\d+)\s*$/.exec(first.headers.get('content-range') || '')?.[1]) : NaN
    if (Number.isFinite(total) && total > 0) {
      if (total > MAX_DOWNLOAD_BYTES) {
        await first.body.cancel().catch(() => {})
        throw new UserError('O áudio é grande demais.')
      }
      await downloadParts(href, file, first, total)
    } else {
      await streamWhole(first, file)
    }
    return file
  } catch (err) {
    if (file) fs.rmSync(file, { force: true })
    throw err
  }
}

function runFfmpeg(file, seconds, filter, nice, threads) {
  return new Promise((resolve, reject) => {
    const args = ['-v', 'error', '-nostdin', '-threads', String(threads), '-i', path.resolve(file), '-vn', '-t', String(seconds)]
    if (filter) args.push('-af', filter)
    args.push('-ac', '1', '-ar', String(SAMPLE_RATE), '-f', 's16le', '-')

    const opts = { stdio: ['ignore', 'pipe', 'pipe'] }
    const p = nice ? spawn('nice', ['-n', '19', 'ffmpeg', ...args], opts) : spawn('ffmpeg', args, opts)
    const chunks = []
    let err = ''
    p.stdout.on('data', (b) => chunks.push(b))
    p.stderr.on('data', (b) => {
      if (err.length < 2000) err += b.toString()
    })
    p.on('error', (e) => reject(new UserError(`Não consegui iniciar o ffmpeg: ${e.message}`)))
    p.on('close', (code) => {
      if (code !== 0) return reject(new Error(`O ffmpeg falhou: ${err.trim() || code}`))
      const raw = Buffer.concat(chunks)
      const even = raw.length - (raw.length % 2)
      if (!even) return reject(new UserError('O arquivo baixado não tem áudio.'))
      const pcm = new Int16Array(even >> 1)
      Buffer.from(pcm.buffer).set(raw.subarray(0, even))
      resolve(pcm)
    })
  })
}

async function decodeToPcm(file, seconds, nice) {
  const loud = NORMALIZE_AUDIO ? 'loudnorm=I=-16:TP=-1.5:LRA=11' : ''
  const soxr = `aresample=${SAMPLE_RATE}:resampler=soxr:precision=28:dither_method=triangular_hp`
  const candidates = [[loud, soxr].filter(Boolean).join(','), loud, ''].filter((f, i, arr) => arr.indexOf(f) === i)
  const threads = 2

  let lastErr
  for (const filter of candidates) {
    try {
      return await runFfmpeg(file, seconds, filter, nice, threads)
    } catch (err) {
      if (err instanceof UserError) throw err
      lastErr = err
      console.warn('ffmpeg: filtro não funcionou, tentando o próximo:', filter || '(nenhum)')
    }
  }
  throw lastErr
}

async function workerEntry() {
  const job = workerData
  try {
    const file = await downloadAudio(job.url)
    if (job.direct) {
      parentPort.postMessage({ ok: true, file })
      return
    }
    try {
      const pcm = await decodeToPcm(file, job.seconds, job.nice)
      parentPort.postMessage({ ok: true, buffer: pcm.buffer }, [pcm.buffer])
    } finally {
      if (!KEEP_AUDIOS) fs.rmSync(file, { force: true })
    }
  } catch (err) {
    parentPort.postMessage({ ok: false, user: err instanceof UserError, message: err?.message ?? String(err) })
  }
}

if (!isMainThread) {
  await workerEntry()
} else {
  const { Client, ProtocolLogger, areJidsSameUser, serialize } = await import('velowa')

  const client = new Client({
    authDir: 'auth',
    logger: new ProtocolLogger({ enabled: false }),
    antiban: true,
  })

  process.on('unhandledRejection', (err) => console.error('Erro não tratado:', err))
  process.on('uncaughtException', (err) => console.error('Exceção não tratada:', err))

  // ---------- Modo de uso ----------
  const MODE_FILE = 'data/modo.json'

  function loadMode() {
    try {
      const saved = JSON.parse(fs.readFileSync(MODE_FILE, 'utf8')).modo
      if (saved === 'self' || saved === 'publico') return saved
    } catch {}
    return process.env.MODO === 'self' ? 'self' : 'publico'
  }

  function saveMode(value) {
    mode = value
    try {
      fs.mkdirSync('data', { recursive: true })
      fs.writeFileSync(MODE_FILE, JSON.stringify({ modo: value }))
    } catch (err) {
      console.error('Não consegui salvar o modo:', err)
    }
  }

  let mode = loadMode()
  const canRun = (m) => mode === 'publico' || isSelf(m)

  // ---------- Conexão ----------
  client.on('connection', (u) => {
    if (u.qr) {
      console.log('\nEscaneie o QR code: WhatsApp > Aparelhos conectados > Conectar um aparelho\n')
      qrcode.generate(u.qr, { small: true })
    }

    if (u.state === 'open') {
      console.log('✅ Bot online como', client.me?.id, `(modo ${mode})`)
    }

    if (u.state === 'close') {
      console.log('Conexão encerrada:', u.reason)
      if (u.loggedOut) {
        console.log('O aparelho foi desconectado pelo celular. Apague a pasta "auth" e rode de novo para gerar um novo QR.')
      }
    }
  })

  // ---------- Utilidades ----------
  const numberOf = (jid) => String(jid).split('@')[0].split(':')[0]

  const isMe = (jid) => {
    const me = client.me
    if (!me || !jid) return false
    return areJidsSameUser(jid, me.id) || (!!me.lid && areJidsSameUser(jid, me.lid))
  }

  const isSelf = (m) => !!m.fromMe || isMe(m.sender)

  const isOwner = (m) => isSelf(m) || OWNERS.includes(numberOf(m.sender))

  function chatOf(m) {
    const chat = m.chat
    const me = client.me
    if (me && String(chat).endsWith('@lid') && me.lid && numberOf(chat) === numberOf(me.lid)) {
      return `${numberOf(me.id)}@s.whatsapp.net`
    }
    return chat
  }

  async function say(m, content) {
    return client.send(chatOf(m), typeof content === 'string' ? { text: content } : content, { quoted: m.asQuoted })
  }

  async function requireOwner(m) {
    if (isOwner(m)) return true
    await say(m, 'Esse comando é só para o dono do bot.')
    return false
  }

  const formatTime = (seconds) => `${Math.floor(seconds / 60)}:${String(Math.round(seconds % 60)).padStart(2, '0')}`

  // ---------- Estado das ligações ----------
  const sessions = new Map()
  const byTarget = new Map()
  const starting = new Set()
  const groupCalls = new Map()
  let activePrep = 0

  if (!KEEP_AUDIOS) {
    try {
      for (const f of fs.readdirSync(MEDIA_DIR)) fs.rmSync(path.join(MEDIA_DIR, f), { force: true })
    } catch {}
  }

  // ---------- Busca e preparo da música ----------
  function hasFfmpeg() {
    const r = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' })
    return !r.error && r.status === 0
  }

  const HAS_NICE = (() => {
    const r = spawnSync('nice', ['-n', '19', 'true'], { stdio: 'ignore' })
    return !r.error && r.status === 0
  })()

  async function fetchAudioInfo(query) {
    const res = await fetch(API_URL + encodeURIComponent(query), {
      signal: AbortSignal.timeout(60_000),
      headers: { 'user-agent': USER_AGENT },
    })
    if (!res.ok) throw new UserError(`A API respondeu com erro (${res.status}).`)
    const data = await res.json().catch(() => null)
    const result = data?.result
    const url = result?.audio?.url
    if (!data?.status || typeof url !== 'string') throw new UserError('A API não encontrou áudio para essa busca.')
    return { title: String(result.title || 'áudio'), duration: Number(result.duration) || 0, url }
  }

  function prepareInWorker(url) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(new URL(import.meta.url), {
        workerData: { url, seconds: MAX_DURATION, nice: HAS_NICE, direct: DIRECT_PLAY },
      })
      let settled = false
      worker.once('message', (msg) => {
        settled = true
        if (msg.ok) resolve(msg.file ? { file: msg.file } : { pcm: new Int16Array(msg.buffer) })
        else reject(msg.user ? new UserError(msg.message) : new Error(msg.message))
      })
      worker.once('error', (err) => {
        if (settled) return
        settled = true
        reject(err)
      })
      worker.once('exit', (code) => {
        if (!settled) reject(new Error(`A thread de download terminou com código ${code}.`))
      })
    })
  }

  async function prepareTrack(query, notify) {
    activePrep++
    try {
      const info = await fetchAudioInfo(query)
      if (info.duration && info.duration > MAX_DURATION) {
        throw new UserError(`Essa música tem ${formatTime(info.duration)} e o limite é ${formatTime(MAX_DURATION)}.`)
      }

      await notify?.(`⬇️ Baixando: ${info.title}`)
      const prepared = await prepareInWorker(info.url)
      const duration = info.duration || (prepared.pcm ? prepared.pcm.length / SAMPLE_RATE : 0)
      return { title: info.title, duration, ...prepared }
    } finally {
      activePrep--
    }
  }

  // ---------- Alvo da ligação ----------
  function targetOf(m) {
    if (m.isGroup) return m.chat
    return isSelf(m) ? chatOf(m) : m.sender
  }

  function callTarget(m) {
    const target = targetOf(m)
    if (!m.isGroup && isMe(target)) {
      throw new UserError('O bot não consegue ligar para o próprio número. Mande o comando em outra conversa.')
    }
    return target
  }

  // ---------- Detecção de ligações de grupo ----------
  const groupOf = (c) =>
    [c.groupJid, c.group, c.groupId, c.chat, c.from]
      .map((x) => (x ? String(x) : ''))
      .find((x) => x.endsWith('@g.us')) || null

  function trackGroupCall(c) {
    if (c.status === 'terminate') {
      for (const [g, info] of groupCalls) if (info.callId === c.callId) groupCalls.delete(g)
      return
    }
    if (c.status !== 'offer' || c.lightweight) return
    const g = groupOf(c)
    if (!g || sessions.has(c.callId)) return
    if (isMe(c.creator ?? c.from)) return
    groupCalls.set(g, { callId: c.callId, at: Date.now() })
  }

  function findJoinableGroupCall(group) {
    const info = groupCalls.get(group)
    if (!info) return null
    if (Date.now() - info.at > GROUP_CALL_TTL_MS || sessions.has(info.callId)) {
      groupCalls.delete(group)
      return null
    }
    return info
  }

  // ---------- Sessões ----------
  const FINISH_NOTICES = {
    done: '✅ A fila acabou e a ligação foi encerrada.',
    musicend: '✅ A música acabou e a ligação foi encerrada.',
    stopped: '⏹️ Música parada e o bot saiu da chamada.',
    queueend: '⏭️ Última música pulada. A fila está vazia, então o bot saiu da chamada.',
    hangup: 'A ligação foi encerrada do outro lado.',
    noanswer: 'Ninguém atendeu a ligação.',
    rejected: 'A ligação foi recusada.',
    ended: 'A ligação terminou antes de ser atendida.',
    timeout: 'A ligação passou do tempo limite e foi encerrada.',
    pausetimeout: '⏱️ Ficou pausado por tempo demais, então o bot saiu da chamada.',
    error: 'Deu erro durante a ligação, ela foi encerrada.',
  }

  function createSession({ callId, target, m, isGroup, joined }) {
    const s = {
      callId,
      target,
      m,
      isGroup,
      joined,
      queue: [],
      direct: DIRECT_PLAY,
      track: null,
      rate: 0,
      current: null,
      offset: 0,
      nowTitle: '',
      started: false,
      startedAt: 0,
      paused: false,
      pausedAt: 0,
      draining: false,
      pumping: false,
      done: false,
    }
    sessions.set(callId, s)
    byTarget.set(target, s)
    return s
  }

  async function finish(s, reason) {
    if (s.done) return
    s.done = true
    clearTimeout(s.ringTimer)
    clearTimeout(s.groupStartTimer)
    clearTimeout(s.maxTimer)
    clearTimeout(s.tailTimer)
    clearTimeout(s.endTimer)
    clearInterval(s.pollTimer)
    sessions.delete(s.callId)
    if (byTarget.get(s.target) === s) byTarget.delete(s.target)
    s.queue.length = 0
    s.current = null
    if (s.direct && s.track?.file && !KEEP_AUDIOS) fs.rm(s.track.file, { force: true }, () => {})

    if (reason !== 'hangup' && reason !== 'rejected' && reason !== 'ended') {
      try {
        await client.endCall(s.callId)
      } catch (err) {
        console.error('Erro ao desligar:', err?.message ?? err)
      }
    }

    const notice = FINISH_NOTICES[reason]
    if (notice) await say(s.m, notice).catch(() => {})
  }

  const lowpassCache = new Map()

  function lowpassFor(ratio) {
    if (lowpassCache.has(ratio)) return lowpassCache.get(ratio)
    const half = 4 * ratio
    const cutoff = (0.5 / ratio) * 0.92
    const taps = new Float64Array(2 * half + 1)
    let sum = 0
    for (let i = -half; i <= half; i++) {
      const sinc = i === 0 ? 2 * cutoff : Math.sin(2 * Math.PI * cutoff * i) / (Math.PI * i)
      const window = 0.5 + 0.5 * Math.cos((Math.PI * i) / (half + 1))
      taps[i + half] = sinc * window
      sum += sinc * window
    }
    for (let i = 0; i < taps.length; i++) taps[i] /= sum
    lowpassCache.set(ratio, taps)
    return taps
  }

  function decimate(pcm, start, end, ratio) {
    const taps = lowpassFor(ratio)
    const half = (taps.length - 1) / 2
    const n = Math.floor((end - start) / ratio)
    const out = new Int16Array(n)
    for (let k = 0; k < n; k++) {
      const center = start + k * ratio
      let acc = 0
      for (let t = -half; t <= half; t++) {
        const idx = center + t
        if (idx >= 0 && idx < pcm.length) acc += pcm[idx] * taps[t + half]
      }
      out[k] = acc > 32767 ? 32767 : acc < -32768 ? -32768 : Math.round(acc)
    }
    return out
  }

  async function probeRate(s) {
    if (SAMPLE_RATE === FALLBACK_RATE) return SAMPLE_RATE
    const probe = new Int16Array(SAMPLE_RATE / 10)
    const before = client.queuedAudioSeconds(s.callId)
    await client.playInCall(s.callId, probe)
    const queued = client.queuedAudioSeconds(s.callId) - before
    const measured = queued > 0 ? probe.length / queued : 0
    const rate = measured && Math.abs(measured - SAMPLE_RATE) / SAMPLE_RATE < 0.15 ? SAMPLE_RATE : FALLBACK_RATE
    console.log(`Taxa de áudio da ligação: ${rate} Hz`)
    return rate
  }

  async function pump(s) {
    if (s.done || !s.started || s.pumping) return
    s.pumping = true
    try {
      if (Date.now() - s.startedAt > 3000) {
        const call = client.calls.get(s.callId)
        if (!call || call.status === 'ended') {
          await finish(s, 'hangup')
          return
        }
      }

      if (s.paused) {
        if (Date.now() - s.pausedAt > PAUSE_TIMEOUT_MS) await finish(s, 'pausetimeout')
        return
      }

      if (!s.rate) s.rate = await probeRate(s)
      const ratio = SAMPLE_RATE / s.rate
      const lookahead = activePrep > 0 ? LOOKAHEAD_BUSY_SECONDS : LOOKAHEAD_SECONDS
      let left = client.queuedAudioSeconds(s.callId)
      while (left < lookahead && !s.done) {
        if (!s.current) {
          s.current = s.queue.shift() ?? null
          s.offset = 0
          if (!s.current) break
          s.nowTitle = s.current.title
          void say(s.m, `▶️ *Tocando agora:* ${s.current.title} (${formatTime(s.current.duration)})`).catch(() => {})
        }

        const track = s.current
        const end = Math.min(s.offset + CHUNK_SAMPLES, track.pcm.length)
        const chunk = ratio === 1 ? track.pcm.slice(s.offset, end) : decimate(track.pcm, s.offset, end, ratio)
        s.offset = end
        if (s.offset >= track.pcm.length) s.current = null
        if (!chunk.length) continue

        await client.playInCall(s.callId, chunk)
        left += chunk.length / s.rate
      }
      if (s.done) return

      const remaining = client.queuedAudioSeconds(s.callId)
      if (!s.current && !s.queue.length && !remaining) {
        if (!s.draining) {
          s.draining = true
          s.tailTimer = setTimeout(() => finish(s, 'done'), HANGUP_TAIL_MS)
        }
      } else if (s.draining) {
        s.draining = false
        clearTimeout(s.tailTimer)
      }
    } catch (err) {
      console.error('Erro ao tocar na ligação:', err)
      await finish(s, 'error')
    } finally {
      s.pumping = false
    }
  }

  async function onAnswered(s) {
    if (s.started || s.done) return
    s.started = true
    s.startedAt = Date.now()
    clearTimeout(s.ringTimer)
    clearTimeout(s.groupStartTimer)
    s.maxTimer = setTimeout(() => finish(s, 'timeout'), MAX_SESSION_MS)

    if (s.direct) {
      const seconds = s.track?.duration || MAX_DURATION
      s.endTimer = setTimeout(() => finish(s, 'musicend'), seconds * 1000 + DIRECT_END_MARGIN_MS)
      s.pollTimer = setInterval(() => {
        if (Date.now() - s.startedAt < 3000) return
        const call = client.calls.get(s.callId)
        if (!call || call.status === 'ended') void finish(s, 'hangup')
      }, 1000)
      void say(s.m, `▶️ *Tocando agora:* ${s.nowTitle} (${formatTime(seconds)})`).catch(() => {})
      return
    }

    s.pollTimer = setInterval(() => void pump(s), PUMP_INTERVAL_MS)
    void pump(s)
  }

  client.on('call', (c) => {
    try {
      if (DEBUG_CALLS) {
        let dump = ''
        try {
          dump = JSON.stringify(c, (_k, v) => (typeof v === 'bigint' ? String(v) : v)).slice(0, 700)
        } catch {}
        console.log('[call]', c.status, dump)
      }
      trackGroupCall(c)

      const s = sessions.get(c.callId)
      if (!s || s.done) return

      if (c.status === 'accept' || (s.isGroup && c.status === 'group_update')) {
        void onAnswered(s)
      } else if (c.status === 'reject') {
        if (!s.isGroup) void finish(s, 'rejected')
      } else if (c.status === 'terminate') {
        void finish(s, s.started ? 'hangup' : 'ended')
      } else if (c.status === 'timeout') {
        if (!s.isGroup) void finish(s, s.started ? 'hangup' : 'ended')
      }
    } catch (err) {
      console.error('Erro ao tratar evento de ligação:', err)
    }
  })

  // ---------- /play ----------
  function attachTrack(s, track) {
    if (s.direct) {
      s.track = track
      s.nowTitle = track.title
    } else {
      s.queue.push(track)
    }
  }

  async function startSession(m, target, track) {
    const audioOpts = DIRECT_PLAY ? { audioFile: path.resolve(track.file) } : undefined
    if (m.isGroup) {
      const joinable = findJoinableGroupCall(target)
      if (joinable) {
        try {
          await say(m, '📞 Detectei uma ligação em andamento no grupo, entrando…')
          await (audioOpts ? client.acceptCall(joinable.callId, audioOpts) : client.acceptCall(joinable.callId))
          groupCalls.delete(target)
          const s = createSession({ callId: joinable.callId, target, m, isGroup: true, joined: true })
          attachTrack(s, track)
          void onAnswered(s)
          return
        } catch (err) {
          console.error('Não consegui entrar na ligação do grupo:', err?.message ?? err)
          groupCalls.delete(target)
          await say(m, 'Não consegui entrar nessa ligação, vou iniciar uma nova no grupo…').catch(() => {})
        }
      }

      await say(m, '📞 Ligando para o grupo… entrem na chamada para ouvir a música.')
      const call = await client.startGroupCall([], { group: target, ...audioOpts })
      const s = createSession({ callId: call.callId, target, m, isGroup: true, joined: false })
      attachTrack(s, track)
      s.groupStartTimer = setTimeout(() => void onAnswered(s), GROUP_START_FALLBACK_MS)
      s.ringTimer = setTimeout(() => finish(s, 'noanswer'), RING_TIMEOUT_GROUP_MS)
      return
    }

    await say(m, '📞 Ligando… atenda para ouvir a música.')
    const call = await (audioOpts ? client.startCall(target, audioOpts) : client.startCall(target))
    const s = createSession({ callId: call.callId, target, m, isGroup: false, joined: false })
    attachTrack(s, track)
    s.ringTimer = setTimeout(() => finish(s, 'noanswer'), RING_TIMEOUT_MS)
  }

  async function playCmd(m, args) {
    const query = args.join(' ').trim()
    if (!query) {
      await say(m, `Use: ${PREFIX}play nome da música  (ex: ${PREFIX}play Bad Boys)`)
      return
    }

    const target = callTarget(m)
    const notify = (text) => say(m, text)

    const existing = byTarget.get(target)
    if (existing) {
      if (DIRECT_PLAY) {
        await say(m, `Já tem uma música tocando aqui. Use ${PREFIX}stop e depois ${PREFIX}play de novo.`)
        return
      }
      if (existing.queue.length >= MAX_QUEUE) {
        await say(m, `A fila está cheia (máximo de ${MAX_QUEUE} músicas).`)
        return
      }
      await say(m, `🔎 Buscando: ${query}…`)
      const track = await prepareTrack(query, notify)
      if (existing.done) throw new UserError(`A ligação acabou enquanto eu preparava a música. Mande ${PREFIX}play de novo.`)
      if (existing.queue.length >= MAX_QUEUE) throw new UserError(`A fila está cheia (máximo de ${MAX_QUEUE} músicas).`)
      existing.queue.push(track)
      await say(m, `➕ *Na fila (#${existing.queue.length}):* ${track.title} (${formatTime(track.duration)})`)
      return
    }

    if (starting.has(target)) {
      await say(m, 'Já estou preparando uma música para essa conversa. Aguarde um instante.')
      return
    }
    if (sessions.size >= MAX_ACTIVE_CALLS) {
      await say(m, 'O bot já está com ligações demais ao mesmo tempo. Tente de novo em instantes.')
      return
    }
    if (!DIRECT_PLAY && !hasFfmpeg()) {
      throw new UserError('O ffmpeg não está instalado. No Termux: pkg install ffmpeg')
    }

    starting.add(target)
    let track = null
    try {
      await say(m, `🔎 Buscando: ${query}…`)
      track = await prepareTrack(query, notify)
      await startSession(m, target, track)
    } catch (err) {
      if (DIRECT_PLAY && track?.file && !KEEP_AUDIOS) fs.rmSync(track.file, { force: true })
      throw err
    } finally {
      starting.delete(target)
    }
  }

  // ---------- Controles ----------
  async function needSession(m, allowUnstarted = false) {
    const s = byTarget.get(targetOf(m))
    if (!s || s.done) {
      await say(m, `Não há nenhuma ligação do bot ativa aqui. Use ${PREFIX}play para começar.`)
      return null
    }
    if (!s.started && !allowUnstarted) {
      await say(m, 'A ligação ainda não foi atendida.')
      return null
    }
    return s
  }

  async function pauseCmd(m) {
    const s = await needSession(m)
    if (!s) return
    if (s.paused) {
      await say(m, `Já está pausado. Use ${PREFIX}continuar para voltar.`)
      return
    }
    s.paused = true
    s.pausedAt = Date.now()
    await say(m, `⏸️ Pausado. O bot fica mudo até alguém usar ${PREFIX}continuar.`)
  }

  async function resumeCmd(m) {
    const s = await needSession(m)
    if (!s) return
    if (!s.paused) {
      await say(m, 'A música não está pausada.')
      return
    }
    s.paused = false
    await say(m, '▶️ Continuando a música.')
  }

  async function skipCmd(m) {
    const s = await needSession(m)
    if (!s) return
    if (!s.queue.length) {
      await finish(s, 'queueend')
      return
    }
    s.current = null
    s.offset = 0
    s.paused = false
    await say(m, '⏭️ Pulando para a próxima da fila…')
  }

  async function stopCmd(m) {
    const s = await needSession(m, true)
    if (!s) return
    await finish(s, 'stopped')
  }

  async function queueCmd(m) {
    const s = await needSession(m)
    if (!s) return
    const lines = [`🎶 *Agora:* ${s.nowTitle || '—'}${s.paused ? ' (pausado)' : ''}`]
    if (s.queue.length) {
      lines.push('', '📋 *Fila:*')
      s.queue.forEach((t, i) => lines.push(`${i + 1}. ${t.title} (${formatTime(t.duration)})`))
    } else {
      lines.push('', 'A fila está vazia.')
    }
    await say(m, lines.join('\n'))
  }

  async function directBlock(m) {
    await say(m, `Esse comando não funciona no modo direto. Use ${PREFIX}stop para parar a música.`)
  }

  // ---------- Comandos ----------
  async function handleCommand(m, command, args) {
    switch (command) {
      case 'menu':
      case 'help':
      case 'ajuda':
      case 'comandos':
      case 'start':
      const menuText = `

👀 ZizysBot – *${m.pushName}*
_Um bot que toca músicas em call do WhatsApp._
   
Comando: *${PREFIX}play <música>*
Busca a música e toca na ligação.

Comando: *${PREFIX}pause*
Pausar uma música na ligação.

Comando: *${PREFIX}continuar*
Retoma a música pausada.

Comando: *${PREFIX}skip*
Pula para a próxima música da fila.

Comando: *${PREFIX}stop*
Para a música e o bot sai da chamada.

Comando: *${PREFIX}fila*
Mostra a música atual e a fila.

> Desenvolvido por *${CREDITS}*
> Link desse projeto (GitHub): https://github.com/maurikkkkj/ZizysBot

`
await say(m, menuText)
break

      case 'oi':
        await say(m, 'opa')
        break

      case 'self':
        if (!(await requireOwner(m))) break
        saveMode('self')
        await say(m, 'Modo self ativado: só o número do bot executa comandos.')
        break

      case 'publico':
        if (!(await requireOwner(m))) break
        saveMode('publico')
        await say(m, 'Modo público ativado: todos podem executar comandos.')
        break

      case 'play':
        await playCmd(m, args)
        break

      case 'pause':
      case 'pausar':
        await (DIRECT_PLAY ? directBlock(m) : pauseCmd(m))
        break

      case 'continuar':
      case 'resume':
      case 'retomar':
        await (DIRECT_PLAY ? directBlock(m) : resumeCmd(m))
        break

      case 'skip':
      case 'pular':
      case 'next':
        await (DIRECT_PLAY ? directBlock(m) : skipCmd(m))
        break

      case 'stop':
      case 'parar':
      case 'sair':
        await stopCmd(m)
        break

      case 'fila':
      case 'queue':
        await (DIRECT_PLAY ? directBlock(m) : queueCmd(m))
        break
    }
  }

  // ---------- Mensagens ----------
  client.on('message', async (raw) => {
    let m
    try {
      m = serialize(client, raw)

      if (!canRun(m)) return
      if (Date.now() / 1000 - m.timestamp > MAX_MESSAGE_AGE_SECONDS) return
      if (!m.text.startsWith(PREFIX)) return

      const [word, ...args] = m.text.slice(PREFIX.length).trim().split(/\s+/)
      if (!word) return

      await handleCommand(m, word.toLowerCase(), args)
    } catch (err) {
      console.error('Erro ao tratar mensagem:', err)
      if (m && canRun(m)) {
        const msg = err instanceof UserError ? err.message : `Não consegui executar o comando: ${err?.message ?? err}`
        await say(m, msg).catch(() => {})
      }
    }
  })

  await client.connect()
}
