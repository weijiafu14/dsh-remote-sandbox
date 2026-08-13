/**
 * Host-side bounded output collector. The sidecar forwards a process's raw
 * output; this holds only a `maxBytes` in-memory tail and, when configured,
 * appends the complete stream to a host spill file up to its own cap. Host
 * memory is therefore bounded regardless of how much a process prints — the
 * guarantee the E2B SDK transport cannot make because it retains the whole
 * stream in memory. Reads are offset-based so independent readers never consume
 * one another's output.
 * @module dsh-subprocess-remote/collected
 */

import { appendFileSync, closeSync, openSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { SubprocessOutputRead, SubprocessOutputReader } from '@deepseek-ai/dsh-subprocess'

let spillCounter = 0

/** A bounded, offset-addressable view over one collected output stream. */
export class CollectedStream implements SubprocessOutputReader {
  private tailChunks: Buffer[] = []
  private tailBytes = 0
  private droppedHead = 0
  private total = 0
  private spillFd: number | undefined
  private spillBytes = 0
  private spillOverflowed = false
  readonly spillPath: string | undefined

  constructor(private readonly maxBytes: number, private readonly spillMaxBytes: number | undefined) {
    if (spillMaxBytes !== undefined) {
      this.spillPath = join(tmpdir(), `dsh-remote-spill-${process.pid}-${spillCounter++}.bin`)
      this.spillFd = openSync(this.spillPath, 'w')
    }
  }

  /** Append forwarded bytes, enforcing the in-memory cap and appending to the spill file. */
  push(bytes: Uint8Array): void {
    const buf = Buffer.from(bytes)
    if (this.spillFd !== undefined && !this.spillOverflowed && this.spillMaxBytes !== undefined) {
      if (this.spillBytes + buf.byteLength <= this.spillMaxBytes) {
        appendFileSync(this.spillFd, buf)
        this.spillBytes += buf.byteLength
      } else {
        // A stream larger than the spill cap discards its now-incomplete spill.
        this.spillOverflowed = true
        closeSync(this.spillFd)
        this.spillFd = undefined
      }
    }
    this.tailChunks.push(buf)
    this.tailBytes += buf.byteLength
    this.total += buf.byteLength
    while (this.tailBytes > this.maxBytes && this.tailChunks.length > 0) {
      const head = this.tailChunks[0] as Buffer
      const overBy = this.tailBytes - this.maxBytes
      if (head.byteLength <= overBy) {
        this.tailChunks.shift()
        this.tailBytes -= head.byteLength
        this.droppedHead += head.byteLength
      } else {
        this.tailChunks[0] = head.subarray(overBy)
        this.tailBytes -= overBy
        this.droppedHead += overBy
      }
    }
  }

  /** Close the spill file at process exit. */
  finish(): void {
    if (this.spillFd !== undefined) { closeSync(this.spillFd); this.spillFd = undefined }
  }

  readFrom(fromByte: number): SubprocessOutputRead {
    const from = Math.max(fromByte, this.droppedHead)
    const tail = Buffer.concat(this.tailChunks)
    const text = tail.subarray(from - this.droppedHead).toString('utf-8')
    const read: SubprocessOutputRead = {
      text,
      nextOffset: this.total,
      lossy: fromByte < this.droppedHead,
    }
    if (this.spillPath !== undefined && !this.spillOverflowed) read.spillPath = this.spillPath
    return read
  }
}
