import 'dotenv/config'
import { spawn, spawnSync } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import { syncBuiltinESMExports } from 'node:module'
import path from 'node:path'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'
import qrcode from 'qrcode-terminal'

const IGNORED_FSYNC_CODES = new Set(['EPERM', 'EINVAL', 'EBADF'])

const realFsyncSync = fs.fsyncSync
fs.fsyncSync = (fd) => {
  try {
    return realFsyncSync(fd)
  } catch (err) {
    if (IGNORED_FSYNC_CODES.has(err?.code)) return
    throw err
  }
}

const realFsync = fs.fsync
fs.fsync = (fd, cb) => {
  realFsync(fd, (err) => cb(err && IGNORED_FSYNC_CODES.has(err.code) ? null : err))
}

syncBuiltinESMExports()

// ---------- Configuração ----------
const PREFIX = '/'
const CREDITS = 'mauri'
const MAX_MESSAGE_AGE_SECONDS = 120

const OWNERS = (process.env.OWNER || '').split(',').map((s) => s.trim()).filter(Boolean)

const API_URL = 'https://api.ikyyxd.my.id/search/ytplayv4?q='
const IMGBB_KEY = '7058e8779e7ed461e566789ad41d6966'
const IMGBB_URL = 'https://api.imgbb.com/1/upload'
const CAPTION_API_URL = 'https://api.popcat.xyz/v2/caption'
const CAPTION_MAX_CHARS = 200
const CAPTION_MAX_IMAGE_BYTES = 16 * 1024 * 1024
const POPCAT_BASE = 'https://api.popcat.xyz/v2'
const BRASILIA_OFFSET_MS = -3 * 60 * 60_000
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
const DIRECT_PLAY = false
const DIRECT_END_MARGIN_MS = 4_000
const DEBUG_CALLS = false
const USER_AGENT = 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36'

const STICKER_SIZE = 512
const STICKER_MAX_SECONDS = 15
const STICKER_MAX_BYTES = 1024 * 1024
const STICKER_MAX_INPUT_BYTES = 20 * 1024 * 1024
const MENTION_MAX_CHARS = 1000
const GROUP_SYNC_DELAY_MS = 1500

const SAMPLE_RATE = 48000
const FALLBACK_RATE = 16000
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
  const { Client, ProtocolLogger, areJidsSameUser, isGroupAdmin, serialize } = await import('velowa')

  const client = new Client({
    authDir: 'auth',
    logger: new ProtocolLogger({ enabled: false }),
    antiban: true,
  })

  const isFsyncPermError = (err) => !!err && err.syscall === 'fsync' && IGNORED_FSYNC_CODES.has(err.code)

  process.on('unhandledRejection', (err) => {
    if (isFsyncPermError(err)) return
    console.error('Erro não tratado:', err)
  })

  process.on('uncaughtException', (err) => {
    if (isFsyncPermError(err)) return
    console.error('Exceção não tratada:', err)
  })

  // ---------- Conexão ----------
  client.on('connection', (u) => {
    if (u.qr) {
      console.log('\nEscaneie o QR code: WhatsApp > Aparelhos conectados > Conectar um aparelho\n')
      qrcode.generate(u.qr, { small: true })
    }

    if (u.state === 'open') {
      console.log('✅ Bot online como', client.me?.id)
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

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  const isMe = (jid) => {
    const me = client.me
    if (!me || !jid) return false
    return areJidsSameUser(jid, me.id) || (!!me.lid && areJidsSameUser(jid, me.lid))
  }

  const isSelf = (m) => !!m.fromMe || isMe(m.sender)

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

  function prepareInWorker(url, direct = DIRECT_PLAY) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(new URL(import.meta.url), {
        workerData: { url, seconds: MAX_DURATION, nice: HAS_NICE, direct },
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
    const audioOpts = DIRECT_PLAY ? { audioFile: path.resolve(track.file), sampleRate: 48000 } : undefined
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

  // ---------- /play2 (envia o áudio na conversa) ----------
  const AUDIO_MIMES = {
    mp3: 'audio/mpeg',
    m4a: 'audio/mp4',
    mp4: 'audio/mp4',
    aac: 'audio/aac',
    ogg: 'audio/ogg; codecs=opus',
    opus: 'audio/ogg; codecs=opus',
    wav: 'audio/wav',
    webm: 'audio/webm',
    flac: 'audio/flac',
  }

  async function play2Cmd(m, args) {
    const query = args.join(' ').trim()
    if (!query) {
      await say(m, `Use: ${PREFIX}play2 nome da música  (ex: ${PREFIX}play2 Bad Boys)`)
      return
    }

    await say(m, `🔎 Buscando: ${query}…`)

    let file = null
    activePrep++
    try {
      const info = await fetchAudioInfo(query)
      if (info.duration && info.duration > MAX_DURATION) {
        throw new UserError(`Essa música tem ${formatTime(info.duration)} e o limite é ${formatTime(MAX_DURATION)}.`)
      }

      await say(m, `⬇️ Baixando: ${info.title}`)
      const prepared = await prepareInWorker(info.url, true)
      file = prepared.file

      const ext = path.extname(file).slice(1).toLowerCase()
      const audio = fs.readFileSync(file)
      await say(m, { audio, mimetype: AUDIO_MIMES[ext] || 'audio/mp4', ptt: false })
      await say(m, `🎵 *${info.title}*${info.duration ? ` (${formatTime(info.duration)})` : ''}`)
    } finally {
      activePrep--
      if (file && !KEEP_AUDIOS) fs.rmSync(file, { force: true })
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

  // ---------- /caption ----------
  const looksLikeImage = (x) => {
    if (!x) return false
    const mime = String(x.mimetype ?? x.mime ?? x.msg?.mimetype ?? '').toLowerCase()
    const type = String(x.mtype ?? x.type ?? x.messageType ?? '').toLowerCase()
    return mime.startsWith('image/') || type.includes('image')
  }

  async function downloadFrom(x) {
    const attempts = [
      () => x.download?.(),
      () => x.downloadMedia?.(),
      () => client.downloadMedia?.(x),
      () => client.downloadMedia?.(x.raw ?? x.message ?? x),
      () => client.download?.(x),
    ]
    let lastErr
    for (const attempt of attempts) {
      try {
        const out = await attempt()
        if (Buffer.isBuffer(out) && out.length) return out
        if (out instanceof Uint8Array && out.length) return Buffer.from(out)
      } catch (err) {
        lastErr = err
      }
    }
    if (lastErr) console.error('Falha ao baixar imagem do WhatsApp:', lastErr)
    return null
  }

  async function getImageBuffer(m) {
    const sources = [m.quoted, m].filter(looksLikeImage)
    if (!sources.length) return null
    for (const src of sources) {
      const buffer = await downloadFrom(src)
      if (buffer) return buffer
    }
    return null
  }

  async function uploadToImgbb(buffer) {
    if (!IMGBB_KEY || IMGBB_KEY.startsWith('COLOQUE')) {
      throw new UserError('A chave do imgbb não foi configurada no código (IMGBB_KEY).')
    }
    const body = new URLSearchParams({ image: buffer.toString('base64') })
    const res = await fetch(`${IMGBB_URL}?key=${encodeURIComponent(IMGBB_KEY)}&expiration=600`, {
      method: 'POST',
      body,
      signal: AbortSignal.timeout(60_000),
    })
    const data = await res.json().catch(() => null)
    const url = data?.data?.url
    if (!res.ok || !data?.success || typeof url !== 'string') {
      throw new UserError(`Não consegui enviar a imagem para o imgbb${data?.error?.message ? `: ${data.error.message}` : ` (erro ${res.status})`}.`)
    }
    return url
  }

  async function renderCaption(imageUrl, text) {
    const url =
      `${CAPTION_API_URL}?image=${encodeURIComponent(imageUrl)}` +
      `&text=${encodeURIComponent(text)}&bottom=false&dark=false&fontsize=50`
    const res = await fetch(url, {
      signal: AbortSignal.timeout(60_000),
      headers: { 'user-agent': USER_AGENT },
    })
    if (!res.ok) throw new UserError(`A API de caption respondeu com erro (${res.status}).`)

    const type = String(res.headers.get('content-type') || '').toLowerCase()
    if (type.startsWith('image/')) return Buffer.from(await res.arrayBuffer())

    const data = await res.json().catch(() => null)
    const link = data?.message?.url ?? data?.url ?? data?.image ?? data?.result
    if (typeof link !== 'string' || !/^https?:\/\//.test(link)) {
      throw new UserError(`A API de caption não devolveu uma imagem${data?.error ? `: ${data.error}` : ''}.`)
    }
    const img = await fetch(link, { signal: AbortSignal.timeout(60_000), headers: { 'user-agent': USER_AGENT } })
    if (!img.ok) throw new UserError(`Não consegui baixar a imagem final (erro ${img.status}).`)
    return Buffer.from(await img.arrayBuffer())
  }

  async function captionCmd(m, args) {
    const text = args.join(' ').trim()
    if (!text) {
      await say(m, `Responda uma imagem com: ${PREFIX}caption seu texto aqui`)
      return
    }
    if (text.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }

    const image = await getImageBuffer(m)
    if (!image) {
      await say(m, `Responda uma imagem com ${PREFIX}caption texto, ou envie a imagem com ${PREFIX}caption texto na legenda.`)
      return
    }
    if (image.length > CAPTION_MAX_IMAGE_BYTES) {
      await say(m, 'A imagem é grande demais.')
      return
    }

    await say(m, '🖼️ Criando a legenda…')
    const link = await uploadToImgbb(image)
    const result = await renderCaption(link, text)
    await say(m, { image: result, caption: '✅ Pronto!' })
  }

  // ---------- Comandos popcat (imagem e texto) ----------
  const popcatUrl = (endpoint, params) => `${POPCAT_BASE}/${endpoint}?${new URLSearchParams(params).toString()}`

  async function popcatRequest(url) {
    const res = await fetch(url, {
      signal: AbortSignal.timeout(60_000),
      headers: { 'user-agent': USER_AGENT },
    })
    if (!res.ok) throw new UserError(`A API respondeu com erro (${res.status}).`)

    const type = String(res.headers.get('content-type') || '').toLowerCase()
    if (type.startsWith('image/')) return { image: Buffer.from(await res.arrayBuffer()) }

    const data = await res.json().catch(() => null)
    if (!data || data.error) {
      throw new UserError(`A API não devolveu um resultado${data?.message && typeof data.message === 'string' ? `: ${data.message}` : ''}.`)
    }
    return { data }
  }

  async function popcatImage(url) {
    const out = await popcatRequest(url)
    if (out.image) return out.image

    const link = out.data?.message?.url ?? out.data?.url ?? out.data?.image ?? out.data?.result
    if (typeof link !== 'string' || !/^https?:\/\//.test(link)) {
      throw new UserError('A API não devolveu uma imagem.')
    }
    const img = await fetch(link, { signal: AbortSignal.timeout(60_000), headers: { 'user-agent': USER_AGENT } })
    if (!img.ok) throw new UserError(`Não consegui baixar a imagem final (erro ${img.status}).`)
    return Buffer.from(await img.arrayBuffer())
  }

  async function popcatText(url) {
    const out = await popcatRequest(url)
    const text = out.data?.message?.text
    if (typeof text !== 'string' || !text.trim()) throw new UserError('A API não devolveu nenhum texto.')
    return text
  }

  const splitParts = (args) =>
    args
      .join(' ')
      .split('|')
      .map((s) => s.trim())

  async function textImageCmd(m, args, endpoint, usage) {
    const text = args.join(' ').trim()
    if (!text) {
      await say(m, `Use: ${PREFIX}${usage}`)
      return
    }
    if (text.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    await say(m, '🖼️ Criando a imagem…')
    const image = await popcatImage(popcatUrl(endpoint, { text }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  async function textReplyCmd(m, args, endpoint, usage) {
    const text = args.join(' ').trim()
    if (!text) {
      await say(m, `Use: ${PREFIX}${usage}`)
      return
    }
    if (text.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    const result = await popcatText(popcatUrl(endpoint, { text }))
    await say(m, result)
  }

  async function imageLinkFrom(m, usage) {
    const image = await getImageBuffer(m)
    if (!image) {
      await say(m, `Responda uma imagem com ${usage}, ou envie a imagem com ${usage} na legenda.`)
      return null
    }
    if (image.length > CAPTION_MAX_IMAGE_BYTES) {
      await say(m, 'A imagem é grande demais.')
      return null
    }
    return uploadToImgbb(image)
  }

  async function imageOnlyCmd(m, endpoint, key, command, extra = {}) {
    const usage = `${PREFIX}${command}`
    if (!(await getImageBuffer(m).then((b) => !!b))) {
      await say(m, `Responda uma imagem com ${usage}, ou envie a imagem com ${usage} na legenda.`)
      return
    }
    await say(m, '🖼️ Criando a imagem…')
    const link = await imageLinkFrom(m, usage)
    if (!link) return
    const image = await popcatImage(popcatUrl(endpoint, { [key]: link, ...extra }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  async function gunCmd(m, args) {
    const text = args.join(' ').trim()
    if (!text) {
      await say(m, `Responda uma imagem com: ${PREFIX}gun seu texto aqui`)
      return
    }
    if (text.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    const usage = `${PREFIX}gun texto`
    if (!(await getImageBuffer(m).then((b) => !!b))) {
      await say(m, `Responda uma imagem com ${usage}, ou envie a imagem com ${usage} na legenda.`)
      return
    }
    await say(m, '🖼️ Criando a imagem…')
    const link = await imageLinkFrom(m, usage)
    if (!link) return
    const image = await popcatImage(popcatUrl('gun', { image: link, text }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  async function tweetCmd(m, args) {
    const [name, username, ...rest] = splitParts(args)
    const text = rest.join(' | ').trim()
    const usage = `${PREFIX}tweet nome | username | texto`
    if (!name || !username || !text) {
      await say(m, `Responda uma imagem (será a foto de perfil) com: ${usage}`)
      return
    }
    if (text.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    if (!(await getImageBuffer(m).then((b) => !!b))) {
      await say(m, `Responda uma imagem com: ${usage}`)
      return
    }
    await say(m, '🐦 Criando o tweet…')
    const link = await imageLinkFrom(m, usage)
    if (!link) return
    const image = await popcatImage(popcatUrl('tweet', { name, username: username.replace(/^@/, ''), text, pfp: link }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  async function discordCmd(m, args) {
    const [username, ...rest] = splitParts(args)
    const content = rest.join(' | ').trim()
    const usage = `${PREFIX}discord username | mensagem`
    if (!username || !content) {
      await say(m, `Responda uma imagem (será o avatar) com: ${usage}`)
      return
    }
    if (content.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    if (!(await getImageBuffer(m).then((b) => !!b))) {
      await say(m, `Responda uma imagem com: ${usage}`)
      return
    }
    await say(m, '💬 Criando a mensagem…')
    const link = await imageLinkFrom(m, usage)
    if (!link) return
    const timestamp = new Date(Date.now() + BRASILIA_OFFSET_MS).toISOString().replace(/\.\d{3}Z$/, 'Z')
    const image = await popcatImage(popcatUrl('discord-message', { username, content, avatar: link, color: '#ffcc99', timestamp }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  async function poohCmd(m, args) {
    const [text1, ...rest] = splitParts(args)
    const text2 = rest.join(' | ').trim()
    if (!text1 || !text2) {
      await say(m, `Use: ${PREFIX}pooh texto 1 | texto 2`)
      return
    }
    if (text1.length > CAPTION_MAX_CHARS || text2.length > CAPTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${CAPTION_MAX_CHARS} caracteres).`)
      return
    }
    await say(m, '🖼️ Criando a imagem…')
    const image = await popcatImage(popcatUrl('pooh', { text1, text2 }))
    await say(m, { image, caption: '✅ Pronto!' })
  }

  // ---------- Comandos de grupo ----------
  const asJid = (x) => {
    if (!x) return null
    const s = String(x)
    return s.includes('@') ? s : `${s}@s.whatsapp.net`
  }

  const sameUser = (a, b) => !!a && !!b && numberOf(a) === numberOf(b)

  const participantForms = (p) => [p?.id, p?.lid, p?.phoneNumber, p?.jid].map(asJid).filter(Boolean)

  const findParticipant = (group, jid) =>
    (group?.participants ?? []).find((p) => participantForms(p).some((form) => sameUser(form, jid))) ?? null

  const adminFlag = (p) => !!p?.admin && p.admin !== 'false'

  const ownerFlag = (p) => p?.admin === 'superadmin' || p?.isSuperAdmin === true

  function isAdminOf(group, jid) {
    const p = findParticipant(group, jid)
    if (p && adminFlag(p)) return true
    try {
      return !!isGroupAdmin(group, jid)
    } catch {
      return false
    }
  }

  const botIdentities = () => [client.me?.id, client.me?.lid].filter(Boolean)

  const isBotAdmin = (group) => botIdentities().some((jid) => isAdminOf(group, jid))

  const isBotParticipant = (p) => botIdentities().some((jid) => participantForms(p).some((form) => sameUser(form, jid)))

  const loadGroup = (jid) => client.groups.metadata(jid)

  async function groupAdminContext(m, { needBotAdmin = true } = {}) {
    if (!m.isGroup) {
      await say(m, 'Esse comando só funciona em grupos.')
      return null
    }
    let group
    try {
      group = await loadGroup(m.chat)
    } catch (err) {
      console.error('Não consegui ler os dados do grupo:', err?.message ?? err)
      await say(m, 'Não consegui ler os dados do grupo agora. Tente de novo em instantes.')
      return null
    }
    if (!isSelf(m) && !isAdminOf(group, m.sender)) {
      await say(m, '🚫 Só admins do grupo podem usar esse comando.')
      return null
    }
    if (needBotAdmin && !isBotAdmin(group)) {
      await say(m, '⚠️ Preciso ser admin do grupo para fazer isso.')
      return null
    }
    return group
  }

  function memberTargets(m, args) {
    const list = [...(m.mentionedJids ?? [])]
    if (!list.length && m.quoted?.sender) list.push(m.quoted.sender)
    if (!list.length) {
      for (const arg of args) {
        const digits = arg.replace(/\D/g, '')
        if (digits.length >= 7 && digits.length <= 15) list.push(`${digits}@s.whatsapp.net`)
      }
    }
    const seen = new Set()
    return list.filter((jid) => {
      const key = numberOf(jid)
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
  }

  const tagList = (entries) => entries.map((p) => `@${numberOf(p.id)}`).join(', ')

  const mentionJids = (entries) => entries.map((p) => asJid(p.id))

  function candidateLists(entries) {
    const lists = [
      entries.map((p) => asJid(p.id)),
      entries.map((p) => asJid(p.lid)),
      entries.map((p) => asJid(p.phoneNumber)),
    ]
    const seen = new Set()
    return lists
      .filter((list) => list.every(Boolean))
      .filter((list) => {
        const key = list.join(',')
        if (seen.has(key)) return false
        seen.add(key)
        return true
      })
  }

  function actionConfirmed(group, p, action) {
    const jid = asJid(p.id)
    if (action === 'remove') return !findParticipant(group, jid)
    if (action === 'promote') return isAdminOf(group, jid)
    if (action === 'demote') return !isAdminOf(group, jid)
    return true
  }

  async function applyAction(chat, entries, action) {
    let lastErr = null
    for (const ids of candidateLists(entries)) {
      try {
        await client.groups.updateParticipants(chat, ids, action)
      } catch (err) {
        lastErr = err
        console.error(`Falha em updateParticipants (${action}):`, err?.message ?? err)
        continue
      }
      await sleep(GROUP_SYNC_DELAY_MS)
      let fresh
      try {
        fresh = await loadGroup(chat)
      } catch {
        return { done: entries, failed: [], error: null }
      }
      const done = entries.filter((p) => actionConfirmed(fresh, p, action))
      if (done.length) return { done, failed: entries.filter((p) => !done.includes(p)), error: null }
    }
    return { done: [], failed: entries, error: lastErr }
  }

  async function changeMembers(m, args, action, { usage, filter, done, nothing }) {
    const group = await groupAdminContext(m)
    if (!group) return

    const all = memberTargets(m, args)
    if (!all.length) {
      await say(m, `Use: ${PREFIX}${usage}  (marque o membro, responda a mensagem dele ou digite o número)`)
      return
    }

    const entries = []
    let outside = 0
    for (const jid of all) {
      const p = findParticipant(group, jid)
      if (!p) {
        outside++
        continue
      }
      if (isBotParticipant(p) || entries.includes(p)) continue
      if (!filter(p)) continue
      entries.push(p)
    }

    if (!entries.length) {
      await say(m, outside && outside === all.length ? 'Esse membro não está no grupo.' : nothing)
      return
    }

    const result = await applyAction(m.chat, entries, action)

    if (result.done.length) {
      await say(m, { text: `${done} ${tagList(result.done)}`, mentions: mentionJids(result.done) })
    }
    if (result.failed.length) {
      const reason = result.error?.message ? `\n${result.error.message}` : ''
      await say(m, {
        text: `⚠️ O WhatsApp não confirmou a mudança para: ${tagList(result.failed)}${reason}`,
        mentions: mentionJids(result.failed),
      })
    }
  }

  const removeCmd = (m, args) =>
    changeMembers(m, args, 'remove', {
      usage: 'remover @membro',
      filter: (p) => !ownerFlag(p),
      done: '🚪 Removido(s):',
      nothing: 'Não posso remover esse membro.',
    })

  const promoteCmd = (m, args) =>
    changeMembers(m, args, 'promote', {
      usage: 'promover @membro',
      filter: (p) => !adminFlag(p),
      done: '⭐ Agora é admin:',
      nothing: 'Esse membro já é admin do grupo.',
    })

  const demoteCmd = (m, args) =>
    changeMembers(m, args, 'demote', {
      usage: 'rebaixar @membro',
      filter: (p) => adminFlag(p) && !ownerFlag(p),
      done: '⬇️ Não é mais admin:',
      nothing: 'Esse membro não é admin do grupo (ou é o dono, que não pode ser rebaixado).',
    })

  const participantJid = (p) => {
    const jid = typeof p === 'string' ? p : p?.phoneNumber || p?.id
    if (!jid) return null
    return String(jid).includes('@') ? String(jid) : `${jid}@s.whatsapp.net`
  }

  async function mentionAllCmd(m) {
    const group = await groupAdminContext(m, { needBotAdmin: false })
    if (!group) return

    const text = m.text.replace(/^\S+\s*/, '').trim()
    if (!text) {
      await say(m, `Use: ${PREFIX}mencionar texto`)
      return
    }
    if (text.length > MENTION_MAX_CHARS) {
      await say(m, `O texto é grande demais (máximo de ${MENTION_MAX_CHARS} caracteres).`)
      return
    }

    const mentions = [...new Set((group.participants ?? []).map(participantJid).filter(Boolean))]
    await client.send(chatOf(m), { text, mentions })
  }

  // ---------- /sticker ----------
  const looksLikeVideo = (x) => {
    if (!x) return false
    const mime = String(x.mimetype ?? x.mime ?? x.msg?.mimetype ?? '').toLowerCase()
    const type = String(x.mtype ?? x.type ?? x.messageType ?? '').toLowerCase()
    return mime.startsWith('video/') || type.includes('video')
  }

  const mimeOf = (x) => String(x?.mimetype ?? x?.mime ?? x?.msg?.mimetype ?? '').toLowerCase()

  async function getStickerSource(m) {
    const sources = [m.quoted, m].filter((x) => looksLikeImage(x) || looksLikeVideo(x))
    for (const src of sources) {
      const buffer = await downloadFrom(src)
      if (buffer) return { buffer, video: looksLikeVideo(src), mime: mimeOf(src) }
    }
    return null
  }

  function mediaDuration(file) {
    const r = spawnSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', file], {
      encoding: 'utf8',
      timeout: 15_000,
    })
    if (r.error || r.status !== 0) return 0
    return Number(String(r.stdout).trim()) || 0
  }

  function runStickerFfmpeg(args) {
    return new Promise((resolve, reject) => {
      const p = spawn('ffmpeg', ['-v', 'error', '-nostdin', '-y', ...args], { stdio: ['ignore', 'ignore', 'pipe'] })
      let err = ''
      p.stderr.on('data', (b) => {
        if (err.length < 2000) err += b.toString()
      })
      p.on('error', (e) => reject(new UserError(`Não consegui iniciar o ffmpeg: ${e.message}`)))
      p.on('close', (code) => {
        if (code !== 0) return reject(new Error(`O ffmpeg falhou: ${err.trim() || code}`))
        resolve()
      })
    })
  }

  async function makeSticker({ buffer, video, mime }) {
    if (!hasFfmpeg()) throw new UserError('O ffmpeg não está instalado. No Termux: pkg install ffmpeg')

    fs.mkdirSync(MEDIA_DIR, { recursive: true })
    const id = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`
    const ext = video ? 'mp4' : mime.includes('png') ? 'png' : mime.includes('webp') ? 'webp' : mime.includes('gif') ? 'gif' : 'jpg'
    const input = path.join(MEDIA_DIR, `sticker-${id}.${ext}`)
    const output = path.join(MEDIA_DIR, `sticker-${id}.webp`)
    fs.writeFileSync(input, buffer)

    try {
      if (video) {
        const duration = mediaDuration(path.resolve(input))
        if (duration > STICKER_MAX_SECONDS + 0.5) {
          throw new UserError(`O vídeo tem ${Math.round(duration)}s e o limite é de ${STICKER_MAX_SECONDS}s.`)
        }
      }

      const size = STICKER_SIZE
      const fit = `scale=${size}:${size}:flags=lanczos,setsar=1,format=rgba`
      const qualities = video ? [55, 40, 28, 18] : [80, 60, 40]

      for (const q of qualities) {
        const args = video
          ? ['-i', input, '-t', String(STICKER_MAX_SECONDS), '-vf', `fps=15,${fit}`, '-an', '-c:v', 'libwebp', '-loop', '0', '-q:v', String(q), '-compression_level', '6', output]
          : ['-i', input, '-vf', fit, '-frames:v', '1', '-c:v', 'libwebp', '-q:v', String(q), output]
        await runStickerFfmpeg(args)
        const out = fs.readFileSync(output)
        if (out.length <= STICKER_MAX_BYTES) return out
      }
      throw new UserError('Não consegui deixar a figurinha leve o bastante. Tente um vídeo menor ou mais curto.')
    } finally {
      fs.rmSync(input, { force: true })
      fs.rmSync(output, { force: true })
    }
  }

  async function stickerCmd(m) {
    const src = await getStickerSource(m)
    if (!src) {
      await say(m, `Responda uma foto, vídeo ou gif com ${PREFIX}sticker, ou envie a mídia com ${PREFIX}sticker na legenda. (vídeo e gif até ${STICKER_MAX_SECONDS}s)`)
      return
    }
    if (src.buffer.length > STICKER_MAX_INPUT_BYTES) {
      await say(m, 'A mídia é grande demais.')
      return
    }
    await say(m, '🖼️ Criando a figurinha…')
    const sticker = await makeSticker(src)
    await say(m, { sticker })
  }

  // ---------- Comandos ----------
  async function handleCommand(m, command, args) {
    switch (command) {
      case 'menu':
      case 'help':
      case 'ajuda':
      case 'comandos':
      case 'start': {
        const menuText = `👀 ZizysBot – *${m.pushName}*
   
Comando: *${PREFIX}play <música>*
Busca a música e toca na ligação.

Comando: *${PREFIX}play2 <música>*
Busca a música e envia o áudio na conversa.

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

Comando: *${PREFIX}caption <texto>*
Colocar legenda em alguma imagem.

Comando: *${PREFIX}tweet <nome> | <username> | <texto>*
Gera um tweet falso realista com nome, username, foto e texto. Responda uma imagem para ser a foto de perfil.

Comando: *${PREFIX}archi <texto>*
Gera uma conquista no estilo do Minecraft com o seu texto.

Comando: *${PREFIX}emojip <texto>*
Transforma o seu texto numa bagunça engraçada de emojis.

Comando: *${PREFIX}discord <username> | <mensagem>*
Gera uma mensagem falsa do Discord. Responda uma imagem para ser o avatar.

Comando: *${PREFIX}could <texto>*
"Se essas crianças soubessem ler, ficariam muito chateadas."

Comando: *${PREFIX}supreme <texto>*
Gera a icônica logo da Supreme com o seu texto.

Comando: *${PREFIX}hue*
Gira a tonalidade (hue) de uma imagem. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}nokia*
Coloca a sua imagem na tela de um Nokia. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}cadeia*
Coloca grades de cadeia sobre a imagem. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}sadcat <texto>*
Cria um meme do gato triste.

Comando: *${PREFIX}oogway <texto>*
Cria um meme de "citação do Mestre Oogway".

Comando: *${PREFIX}comunismo*
Coloca um efeito comunista sobre a imagem. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}pooh <texto 1> | <texto 2>*
Cria um meme do Pooh normal e de smoking.

Comando: *${PREFIX}procurado*
Cria um cartaz falso de "procurado" com a sua imagem. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}gun <texto>*
Coloca uma arma sobre a imagem com o texto. Responda uma imagem ou envie com o comando.

Comando: *${PREFIX}mock <texto>*
Transforma o texto em tom sarcástico.

Comando: *${PREFIX}sticker*
Cria uma figurinha de foto, vídeo ou gif (até ${STICKER_MAX_SECONDS}s). Responda a mídia ou envie com o comando.

Comando: *${PREFIX}remover @membro*
Remove um membro do grupo. Só admins. Marque o membro, responda a mensagem dele ou digite o número.

Comando: *${PREFIX}promover @membro*
Promove um membro a admin do grupo. Só admins. Marque o membro, responda a mensagem dele ou digite o número.

Comando: *${PREFIX}rebaixar @membro*
Tira o admin de um membro do grupo. Só admins. Marque o membro, responda a mensagem dele ou digite o número.

Comando: *${PREFIX}mencionar <texto>*
Menciona todos os membros do grupo de forma invisível, mostrando só o texto. Só admins.

> Desenvolvido por mauri | source:
> https://github.com/maurikkkkj/ZizysBot`
        await say(m, menuText)
        break
      }

      case 'oi':
        await say(m, 'opa')
        break

      case 'play':
        await playCmd(m, args)
        break

      case 'play2':
        await play2Cmd(m, args)
        break

      case 'caption':
        await captionCmd(m, args)
        break

      case 'tweet':
        await tweetCmd(m, args)
        break

      case 'archi':
        await textImageCmd(m, args, 'achievement', 'archi texto')
        break

      case 'emojip':
        await textReplyCmd(m, args, 'emojipasta', 'emojip texto')
        break

      case 'discord':
        await discordCmd(m, args)
        break

      case 'could':
        await textImageCmd(m, args, 'couldread', 'could texto')
        break

      case 'supreme':
        await textImageCmd(m, args, 'supreme', 'supreme texto')
        break

      case 'hue':
        await imageOnlyCmd(m, 'hue-rotate', 'img', 'hue', { deg: '120' })
        break

      case 'nokia':
        await imageOnlyCmd(m, 'nokia', 'image', 'nokia')
        break

      case 'cadeia':
        await imageOnlyCmd(m, 'jail', 'image', 'cadeia')
        break

      case 'sadcat':
        await textImageCmd(m, args, 'sadcat', 'sadcat texto')
        break

      case 'oogway':
        await textImageCmd(m, args, 'oogway', 'oogway texto')
        break

      case 'comunismo':
        await imageOnlyCmd(m, 'communism', 'image', 'comunismo')
        break

      case 'pooh':
        await poohCmd(m, args)
        break

      case 'procurado':
        await imageOnlyCmd(m, 'wanted', 'image', 'procurado')
        break

      case 'gun':
        await gunCmd(m, args)
        break

      case 'mock':
        await textReplyCmd(m, args, 'mock', 'mock texto')
        break

      case 'sticker':
      case 'figurinha':
        await stickerCmd(m)
        break

      case 'remover':
        await removeCmd(m, args)
        break

      case 'promover':
        await promoteCmd(m, args)
        break

      case 'rebaixar':
        await demoteCmd(m, args)
        break

      case 'mencionar':
        await mentionAllCmd(m)
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

      if (Date.now() / 1000 - m.timestamp > MAX_MESSAGE_AGE_SECONDS) return
      if (!m.text.startsWith(PREFIX)) return

      const [word, ...args] = m.text.slice(PREFIX.length).trim().split(/\s+/)
      if (!word) return

      await handleCommand(m, word.toLowerCase(), args)
    } catch (err) {
      console.error('Erro ao tratar mensagem:', err)
      if (m) {
        const msg = err instanceof UserError ? err.message : `Não consegui executar o comando: ${err?.message ?? err}`
        await say(m, msg).catch(() => {})
      }
    }
  })

  await client.connect()
}
