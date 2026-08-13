/**
 * WebSocket frame codec. Control frames are JSON text; bulk bytes are binary
 * frames carrying an 8-byte header (kind, stream discriminant, correlation id)
 * so a raw read/write payload never pays base64 inflation and never blocks the
 * JSON control channel. Zero runtime dependencies: only Uint8Array/DataView/JSON.
 * @module dsh-remote-protocol/codec
 */

import { BIN_BYTES_REQUEST, BIN_BYTES_RESPONSE, BIN_STREAM_CHUNK } from './index.js'
import type { ControlFrame } from './index.js'

/** A decoded binary frame: kind, stream discriminant, correlation id, payload. */
export interface BinaryFrame {
  kind: number
  /** 0 = stdout, 1 = stderr, 2 = data/file — meaningful for stream chunks only. */
  stream: number
  id: number
  payload: Uint8Array
}

/** Stream discriminants for binary chunk frames. */
export const STREAM_STDOUT = 0
export const STREAM_STDERR = 1
export const STREAM_DATA = 2

const HEADER_BYTES = 8

/**
 * Serialize a control frame to a JSON string for a text WebSocket message.
 * @param frame - the control frame to encode.
 * @returns the JSON text payload.
 */
export function encodeControl(frame: ControlFrame): string {
  return JSON.stringify(frame)
}

/**
 * Parse a text WebSocket message into a control frame. Throws on malformed JSON
 * or a missing discriminant, since a peer that emits those broke the protocol.
 * @param text - the received text payload.
 * @returns the decoded control frame.
 */
export function decodeControl(text: string): ControlFrame {
  const parsed: unknown = JSON.parse(text)
  if (typeof parsed !== 'object' || parsed === null || typeof (parsed as { t?: unknown }).t !== 'string') {
    throw new Error('remote-protocol: control frame missing discriminant tag')
  }
  return parsed as ControlFrame
}

/**
 * Encode a binary frame: an 8-byte header (1-byte kind, 1-byte stream, 2 bytes
 * reserved, 4-byte big-endian id) followed by the raw payload.
 * @param kind - one of the `BIN_*` frame kinds.
 * @param id - correlation id (request id for bytes, channel id for a stream chunk).
 * @param payload - the raw bytes.
 * @param stream - stream discriminant for chunk frames (default 0 = stdout).
 * @returns a Uint8Array ready for a binary WebSocket message.
 */
export function encodeBinary(kind: number, id: number, payload: Uint8Array, stream = 0): Uint8Array {
  const out = new Uint8Array(HEADER_BYTES + payload.byteLength)
  const view = new DataView(out.buffer)
  view.setUint8(0, kind & 0xff)
  view.setUint8(1, stream & 0xff)
  view.setUint32(4, id >>> 0)
  out.set(payload, HEADER_BYTES)
  return out
}

/**
 * Decode a binary WebSocket message into its header fields and payload view.
 * @param data - the received binary payload.
 * @returns the decoded binary frame; payload is a view over `data`.
 */
export function decodeBinary(data: Uint8Array): BinaryFrame {
  if (data.byteLength < HEADER_BYTES) {
    throw new Error('remote-protocol: binary frame shorter than header')
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const kind = view.getUint8(0)
  const stream = view.getUint8(1)
  const id = view.getUint32(4)
  return { kind, stream, id, payload: data.subarray(HEADER_BYTES) }
}

export { BIN_BYTES_REQUEST, BIN_BYTES_RESPONSE, BIN_STREAM_CHUNK }
