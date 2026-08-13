/**
 * Wire protocol shared by the remote-sandbox sidecar (in the sandbox) and the
 * host-side fs/subprocess/keeper plugins. One WebSocket carries JSON control
 * frames (text) and raw byte frames (binary). Every fs or subprocess operation
 * is one request/response exchange; bulk output streams as correlated chunk
 * frames. The sidecar executes primitives locally, so a host operation that the
 * official E2B provider spends 3-8 SDK commands on costs exactly one round trip
 * here.
 * @module dsh-remote-protocol
 */

export const PROTOCOL_VERSION = 1

/** Default TCP port the sidecar listens on inside the sandbox. */
export const DEFAULT_SIDECAR_PORT = 49201

/** First binary-frame byte: a raw-bytes response correlated to a request id. */
export const BIN_BYTES_RESPONSE = 0x01
/** First binary-frame byte: a raw-bytes request payload (writeBytes / tar-in). */
export const BIN_BYTES_REQUEST = 0x02
/** First binary-frame byte: a streamed binary chunk on a channel (tar-out). */
export const BIN_STREAM_CHUNK = 0x03

// ---------------------------------------------------------------------------
// Control frames (JSON text WebSocket messages)
// ---------------------------------------------------------------------------

/** Host → sidecar: invoke one method. `bin` marks a paired binary request frame. */
export interface ReqFrame {
  t: 'req'
  id: number
  method: MethodName
  params: unknown
  /** True when a binary request frame with the same id follows immediately. */
  bin?: boolean
}

/** Host → sidecar: abort an in-flight request (maps to an AbortSignal). */
export interface CancelFrame {
  t: 'cancel'
  id: number
}

/**
 * Host → sidecar: close the input side of a process channel. Input bytes
 * themselves travel as `BIN_BYTES_REQUEST` binary frames keyed by channel id,
 * so a piped stdin preserves exact bytes (a tar stream, a binary payload).
 */
export interface StdinEndFrame {
  t: 'stdin-end'
  ch: number
}

/** Sidecar → host: successful result for a request. */
export interface ResOkFrame {
  t: 'res'
  id: number
  ok: true
  result: unknown
}

/** Sidecar → host: typed failure for a request. */
export interface ResErrFrame {
  t: 'res'
  id: number
  ok: false
  error: WireError
}

/** Sidecar → host: a channel stream ended (its process/file completed). */
export interface EndFrame {
  t: 'end'
  ch: number
  stream: 'stdout' | 'stderr' | 'data'
}

/** Sidecar → host: an out-of-band lifecycle event keyed by kind. */
export interface EventFrame {
  t: 'event'
  kind: 'proc-exit'
  ch: number
  /** Exit facts for proc-exit. */
  exitCode: number | null
  signal: string | null
}

/** Every JSON control frame the protocol admits. */
export type ControlFrame =
  | ReqFrame
  | CancelFrame
  | StdinEndFrame
  | ResOkFrame
  | ResErrFrame
  | EndFrame
  | EventFrame

/** A typed error carried in a failed response; `code` is a stable machine token. */
export interface WireError {
  code: string
  message: string
}

// ---------------------------------------------------------------------------
// Method catalog (params/results). Method names are stable string tokens.
// ---------------------------------------------------------------------------

export type MethodName =
  | 'sys.hello'
  | 'sys.ping'
  | 'fs.realpath'
  | 'fs.stat'
  | 'fs.lstat'
  | 'fs.readText'
  | 'fs.readTextStream'
  | 'fs.readBytes'
  | 'fs.listDir'
  | 'fs.writeText'
  | 'fs.editText'
  | 'proc.resolveExecutable'
  | 'proc.spawn'
  | 'proc.terminate'
  | 'proc.waitForExit'

/** `sys.hello` result: sidecar identity + protocol handshake. */
export interface HelloResult {
  protocolVersion: number
  sidecarVersion: string
  platform: string
  /** Absolute path of the sidecar's own runtime state dir (never a workspace path). */
  runtimeRoot: string
}

/** Regular stat/dir file type (a followed target is never a symlink). */
export type FsType = 'file' | 'directory' | 'other'
/** lstat file type; the final component may itself be a symlink. */
export type FsLType = 'file' | 'directory' | 'symlink' | 'other'

export interface StatResult {
  type: FsType
  /** Byte size for a regular file. */
  size: number
  /** Opaque version token `mtimeNs:size:ino:dev`; changes on every content write. */
  version: string
}

export interface LstatResult {
  type: FsLType
  size: number
  version: string
}

export interface DirEntry {
  name: string
  type: FsType
  version: string
  size: number
}

/** Mirrors dsh FsWriteOutcome so the provider only brands the version. */
export interface WriteResult {
  operation: 'create' | 'update'
  version: string
  /** LF-normalized prior content, or null when the file was created. */
  before: string | null
  /** LF-normalized new content. */
  after: string
}

/** Mirrors dsh FsEditOutcome; match-count violations are typed errors, not fields. */
export interface EditResult {
  version: string
  before: string
  after: string
}

/** Bytes written; a `writeText`-style intent guards create/replace + staleness. */
export interface WriteTextParams {
  path: string
  content: string
  /** 'createIfAbsent' fails if the path exists; 'replaceIfVersion' guards staleness. */
  intent?: { kind: 'createIfAbsent' } | { kind: 'replaceIfVersion'; version: string }
  /** Sandbox mode fence applied by the sidecar for this write. */
  mode?: string
  workspaceRoot?: string
}

export interface EditTextParams {
  path: string
  oldString: string
  newString: string
  replaceAll: boolean
  expectedVersion?: string
  mode?: string
  workspaceRoot?: string
}

export interface SpawnParams {
  /** Host-allocated channel id correlating this process's output and exit frames. */
  ch: number
  argv: readonly string[]
  cwd: string
  /** Per-stream disposition; 'collect' streams chunks, 'ignore' drops, 'pipe' streams raw. */
  stdout: 'collect' | 'ignore' | 'pipe'
  stderr: 'collect' | 'ignore' | 'pipe'
  stdin: 'ignore' | 'pipe' | { data: string }
  env: Record<string, string>
  graceMs: number
}

export interface SpawnResult {
  ch: number
  pid: number
}

/** Codec entry points, stream discriminants, and frame guards. */
export { encodeControl, decodeControl, encodeBinary, decodeBinary, STREAM_STDOUT, STREAM_STDERR, STREAM_DATA } from './codec.js'
export type { BinaryFrame } from './codec.js'
