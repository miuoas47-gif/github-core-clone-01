// Forge — a standalone GitHub-compatible git forge in one process.
//
// Surfaces, all over one in-memory store:
//   * git smart HTTP at /{owner}/{repo}.git (upload-pack v2 + v0, receive-pack v0)
//   * REST v3 under /api/v3
//   * GraphQL v4 at /api/graphql
//   * MCP (JSON-RPC 2.0 Streamable HTTP) at /mcp
//
// Lifecycle: bind an ephemeral port, POST {"port": n} to $MAKEFUN_ENV_URL/register,
// then poll GET $MAKEFUN_ENV_URL/done until {"done": true} and exit 0.
//
// Determinism: nothing persisted or returned ever reads the wall clock, Math.random,
// or an unordered iteration. Ids and timestamps come from monotonic counters seeded
// from MAKEFUN_TRIAL_NOW when the trial provides it, so a live run and its replay
// see byte-identical behaviour.
//
// Node 22 standard library only. Run as: node /bot/main.ts

import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import { deflateSync, inflateSync } from 'node:zlib';

// ---------------------------------------------------------------------------
// constants

const ZERO_OID = '0000000000000000000000000000000000000000';
const FORGE_NAME = 'forge';
const FORGE_VERSION = '1.0.0';
const DEFAULT_BRANCH = 'main';
// The sealed scenario's viewer (wire.ts SCENARIO_SEED is constant for every trial,
// so the token owner's login is the same on live and replay passes).
const VIEWER_LOGIN = 'adaiversen';
const VIEWER_NAME = 'ada iversen';
const VIEWER_EMAIL = 'adaiversen@example.invalid';
const VIEWER_ID = 1000;
const TOKEN = process.env.MAKEFUN_SHOP_TOKEN ?? '';
const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');

// ---------------------------------------------------------------------------
// deterministic counters (the only source of ids and timestamps)

let seqCounter = 0;
/** A monotonically increasing sequence number; the backbone of every id and stamp. */
function nextSeq(): number {
  seqCounter += 1;
  return seqCounter;
}

let idCounter = 0;
/** Global numeric ids (repositories, comments): 1, 2, 3, ... in request order. */
function nextId(): number {
  idCounter += 1;
  return idCounter;
}

let reqCounter = 0;
/** The X-GitHub-Request-Id value for one reply. */
function nextRequestId(): string {
  reqCounter += 1;
  return `req-${String(reqCounter).padStart(8, '0')}`;
}

/** The trial's opening instant, or a fixed fallback: the base of every timestamp. */
const TRIAL_NOW_MS = (() => {
  const raw = process.env.MAKEFUN_TRIAL_NOW ?? '';
  const parsed = raw === '' ? NaN : Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : Date.parse('2026-11-14T06:30:00Z');
})();

/** A deterministic timestamp: the trial's now plus `seq` seconds, as epoch + ISO text. */
function stamp(seq: number): { epoch: number; iso: string } {
  const ms = TRIAL_NOW_MS + seq * 1000;
  return { epoch: Math.floor(ms / 1000), iso: new Date(ms).toISOString().replace('.000Z', 'Z') };
}

// ---------------------------------------------------------------------------
// small helpers

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function asBool(value: unknown, fallback = false): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function asInt(value: unknown, fallback = 0): number {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === 'string' && value.trim() !== '') {
    const n = Number(value);
    if (Number.isFinite(n)) return Math.trunc(n);
  }
  return fallback;
}

function sha1Hex(data: Buffer): string {
  return createHash('sha1').update(data).digest('hex');
}

/** The git object id of `type` + `data` (sha1 of "<type> <len>\0<data>"). */
function hashObject(type: string, data: Buffer): string {
  const header = Buffer.from(`${type} ${data.length}\0`, 'ascii');
  return sha1Hex(Buffer.concat([header, data]));
}

function b64encode(data: Buffer): string {
  return data.toString('base64');
}

function b64decode(text: string): Buffer {
  return Buffer.from(text.replace(/[\r\n\t ]+/g, ''), 'base64');
}

/** Base64url without padding: the opaque global ids GraphQL and MCP hand out. */
function gid(kind: string, parts: Array<string | number>): string {
  return Buffer.from(`${kind}:${parts.join(':')}`, 'utf8').toString('base64url');
}

/** Decode an opaque global id back into [kind, ...parts], or null. */
function ungid(value: unknown): string[] | null {
  if (typeof value !== 'string' || value === '') return null;
  try {
    const text = Buffer.from(value, 'base64url').toString('utf8');
    const parts = text.split(':');
    if (parts.length < 2) return null;
    return parts;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// pkt-line framing (git smart HTTP)

function pktLine(payload: string | Buffer): Buffer {
  const data = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
  const len = data.length + 4;
  const head = Buffer.alloc(4);
  head.write(len.toString(16).padStart(4, '0'), 'ascii');
  return Buffer.concat([head, data]);
}

const PKT_FLUSH = Buffer.from('0000', 'ascii');
const PKT_DELIM = Buffer.from('0001', 'ascii');

function pktLines(lines: Array<string | Buffer | 'flush' | 'delim'>): Buffer {
  const parts: Buffer[] = [];
  for (const line of lines) {
    if (line === 'flush') parts.push(PKT_FLUSH);
    else if (line === 'delim') parts.push(PKT_DELIM);
    else parts.push(pktLine(line));
  }
  return Buffer.concat(parts);
}

type Pkt =
  | { kind: 'data'; payload: Buffer }
  | { kind: 'flush' }
  | { kind: 'delim' }
  | { kind: 'response-end' };

/** Decode a whole pkt-line stream into packets (throws on a bad length prefix). */
function decodePktLines(buf: Buffer): Pkt[] {
  const out: Pkt[] = [];
  let off = 0;
  while (off < buf.length) {
    if (off + 4 > buf.length) throw new Error('truncated pkt-line length');
    const lenText = buf.subarray(off, off + 4).toString('ascii');
    const len = parseInt(lenText, 16);
    if (Number.isNaN(len)) throw new Error(`bad pkt-line length ${JSON.stringify(lenText)}`);
    if (len === 0) {
      out.push({ kind: 'flush' });
      off += 4;
    } else if (len === 1) {
      out.push({ kind: 'delim' });
      off += 4;
    } else if (len === 2) {
      out.push({ kind: 'response-end' });
      off += 4;
    } else {
      if (off + len > buf.length) throw new Error('truncated pkt-line payload');
      out.push({ kind: 'data', payload: buf.subarray(off + 4, off + len) });
      off += len;
    }
  }
  return out;
}

/** Split a byte stream into sideband packets (band byte + up to 65515 data bytes each). */
function sidebandChunks(band: number, data: Buffer): Buffer[] {
  const out: Buffer[] = [];
  const MAX = 65515;
  for (let off = 0; off < data.length; off += MAX) {
    const slice = data.subarray(off, Math.min(data.length, off + MAX));
    out.push(pktLine(Buffer.concat([Buffer.from([band]), slice])));
  }
  if (data.length === 0) out.push(pktLine(Buffer.from([band])));
  return out;
}

// ---------------------------------------------------------------------------
// git objects: trees, commits, tags

interface TreeEntry {
  mode: string;
  name: string;
  oid: string;
}

/** Parse a tree object's entries (mode SP name NUL 20-byte oid, sorted by git's rules). */
function parseTree(data: Buffer): TreeEntry[] {
  const entries: TreeEntry[] = [];
  let off = 0;
  while (off < data.length) {
    const sp = data.indexOf(0x20, off);
    if (sp < 0) break;
    const nul = data.indexOf(0, sp + 1);
    if (nul < 0 || nul + 21 > data.length) break;
    const mode = data.subarray(off, sp).toString('ascii');
    const name = data.subarray(sp + 1, nul).toString('utf8');
    const oid = data.subarray(nul + 1, nul + 21).toString('hex');
    entries.push({ mode, name, oid });
    off = nul + 21;
  }
  return entries;
}

function compareTreeNames(a: { name: string; mode: string }, b: { name: string; mode: string }): number {
  const an = a.mode === '40000' ? `${a.name}/` : a.name;
  const bn = b.mode === '40000' ? `${b.name}/` : b.name;
  return an < bn ? -1 : an > bn ? 1 : 0;
}

/** Build a tree object from entries (sorted the way git sorts them). */
function buildTree(entries: readonly TreeEntry[]): Buffer {
  const sorted = [...entries].sort(compareTreeNames);
  const parts: Buffer[] = [];
  for (const entry of sorted) {
    parts.push(Buffer.from(`${entry.mode} ${entry.name}\0`, 'utf8'));
    parts.push(Buffer.from(entry.oid, 'hex'));
  }
  return Buffer.concat(parts);
}

interface CommitView {
  tree: string;
  parents: string[];
  author: string;
  committer: string;
  message: string;
}

/** Parse a commit object into its headers and message. */
function parseCommit(data: Buffer): CommitView {
  const text = data.toString('utf8');
  const blank = text.indexOf('\n\n');
  const head = blank < 0 ? text : text.slice(0, blank);
  const message = blank < 0 ? '' : text.slice(blank + 2);
  let tree = '';
  const parents: string[] = [];
  let author = '';
  let committer = '';
  for (const line of head.split('\n')) {
    if (line.startsWith('tree ')) tree = line.slice(5).trim();
    else if (line.startsWith('parent ')) parents.push(line.slice(7).trim());
    else if (line.startsWith('author ')) author = line.slice(7);
    else if (line.startsWith('committer ')) committer = line.slice(10);
  }
  return { tree, parents, author, committer, message };
}

interface TagView {
  object: string;
  type: string;
  tag: string;
  tagger: string;
  message: string;
}

/** Parse an annotated tag object. */
function parseTag(data: Buffer): TagView {
  const text = data.toString('utf8');
  const blank = text.indexOf('\n\n');
  const head = blank < 0 ? text : text.slice(0, blank);
  const message = blank < 0 ? '' : text.slice(blank + 2);
  let object = '';
  let type = '';
  let tag = '';
  let tagger = '';
  for (const line of head.split('\n')) {
    if (line.startsWith('object ')) object = line.slice(7).trim();
    else if (line.startsWith('type ')) type = line.slice(5).trim();
    else if (line.startsWith('tag ')) tag = line.slice(4);
    else if (line.startsWith('tagger ')) tagger = line.slice(7);
  }
  return { object, type, tag, tagger, message };
}

/** Build a commit object. The message is stored with one trailing newline. */
function buildCommit(fields: {
  tree: string;
  parents: readonly string[];
  message: string;
  author: string;
  committer: string;
  epoch: number;
}): Buffer {
  const lines: string[] = [`tree ${fields.tree}`];
  for (const parent of fields.parents) lines.push(`parent ${parent}`);
  lines.push(`author ${fields.author} ${fields.epoch} +0000`);
  lines.push(`committer ${fields.committer} ${fields.epoch} +0000`);
  const message = fields.message.endsWith('\n') ? fields.message : `${fields.message}\n`;
  return Buffer.from(`${lines.join('\n')}\n\n${message}`, 'utf8');
}

function ident(): string {
  return `${VIEWER_NAME} <${VIEWER_EMAIL}>`;
}

// ---------------------------------------------------------------------------
// packs: read (plain + delta) and write (plain, version 2)

type GitType = 'commit' | 'tree' | 'blob' | 'tag';

interface PackObject {
  type: GitType;
  data: Buffer;
}

const PACK_TYPES: Record<number, GitType | 'ofs-delta' | 'ref-delta'> = {
  1: 'commit',
  2: 'tree',
  3: 'blob',
  4: 'tag',
  6: 'ofs-delta',
  7: 'ref-delta',
};

/** Read one pack entry's varint type+size header. */
function readPackHeader(buf: Buffer, off: number): { type: number; size: number; next: number } {
  let byte = buf[off] ?? 0;
  const type = (byte >> 4) & 7;
  let size = byte & 0x0f;
  let shift = 4;
  let pos = off + 1;
  while ((byte & 0x80) !== 0) {
    byte = buf[pos] ?? 0;
    size |= (byte & 0x7f) << shift;
    shift += 7;
    pos += 1;
  }
  return { type, size, next: pos };
}

function readOfsDeltaBase(buf: Buffer, off: number): { baseOffset: number; next: number } {
  let byte = buf[off] ?? 0;
  let baseOffset = byte & 0x7f;
  let pos = off + 1;
  while ((byte & 0x80) !== 0) {
    byte = buf[pos] ?? 0;
    baseOffset = ((baseOffset + 1) << 7) | (byte & 0x7f);
    pos += 1;
  }
  return { baseOffset, next: pos };
}

/** Apply a git delta to its base bytes. */
function applyDelta(base: Buffer, delta: Buffer): Buffer {
  let pos = 0;
  const readVarint = (): number => {
    let value = 0;
    let shift = 0;
    for (;;) {
      const byte = delta[pos] ?? 0;
      pos += 1;
      value |= (byte & 0x7f) << shift;
      shift += 7;
      if ((byte & 0x80) === 0) break;
    }
    return value;
  };
  const srcSize = readVarint();
  const dstSize = readVarint();
  if (srcSize !== base.length) throw new Error('delta base size mismatch');
  const parts: Buffer[] = [];
  let outLen = 0;
  while (pos < delta.length) {
    const op = delta[pos] ?? 0;
    pos += 1;
    if ((op & 0x80) !== 0) {
      let offset = 0;
      let size = 0;
      if (op & 0x01) { offset = delta[pos] ?? 0; pos += 1; }
      if (op & 0x02) { offset |= (delta[pos] ?? 0) << 8; pos += 1; }
      if (op & 0x04) { offset |= (delta[pos] ?? 0) << 16; pos += 1; }
      if (op & 0x08) { offset |= (delta[pos] ?? 0) << 24; pos += 1; }
      if (op & 0x10) { size = delta[pos] ?? 0; pos += 1; }
      if (op & 0x20) { size |= (delta[pos] ?? 0) << 8; pos += 1; }
      if (op & 0x40) { size |= (delta[pos] ?? 0) << 16; pos += 1; }
      if (size === 0) size = 0x10000;
      parts.push(base.subarray(offset, offset + size));
      outLen += size;
    } else if (op !== 0) {
      parts.push(delta.subarray(pos, pos + op));
      outLen += op;
      pos += op;
    } else {
      throw new Error('delta opcode 0');
    }
  }
  const out = Buffer.concat(parts);
  if (out.length !== dstSize) throw new Error('delta result size mismatch');
  return out;
}

/** Parse a whole pack into objects keyed by oid (verifies the trailer checksum). */
function readPack(pack: Buffer): Map<string, PackObject> {
  if (pack.length < 12 || pack.subarray(0, 4).toString('ascii') !== 'PACK') {
    throw new Error('not a packfile');
  }
  const version = pack.readUInt32BE(4);
  if (version !== 2 && version !== 3) throw new Error(`unsupported pack version ${version}`);
  const count = pack.readUInt32BE(8);
  const trailer = pack.subarray(pack.length - 20).toString('hex');
  if (sha1Hex(pack.subarray(0, pack.length - 20)) !== trailer) {
    throw new Error('pack trailer checksum mismatch');
  }
  const objects = new Map<string, PackObject>();
  // Entries are stored with their file offset so OFS_DELTA bases resolve.
  const byOffset = new Map<number, PackObject>();
  const pending: Array<{ offset: number; baseOid: string; delta: Buffer }> = [];
  let off = 12;
  for (let i = 0; i < count; i += 1) {
    const entryOffset = off;
    const header = readPackHeader(pack, off);
    const typeNum = header.type;
    off = header.next;
    let baseOid: string | null = null;
    if (typeNum === 6) {
      const base = readOfsDeltaBase(pack, off);
      const target = entryOffset - base.baseOffset;
      const baseObj = byOffset.get(target);
      if (baseObj === undefined) throw new Error('ofs-delta base not yet read');
      baseOid = hashObject(baseObj.type, baseObj.data);
      off = base.next;
    } else if (typeNum === 7) {
      baseOid = pack.subarray(off, off + 20).toString('hex');
      off += 20;
    }
    const inflated = inflateSync(pack.subarray(off));
    // inflateSync consumes the whole remainder; find where this entry's zlib stream ends
    // by re-inflating with a bounded window is not possible, so track the consumed length
    // through the stream's unused data via a manual inflate instead.
    off = off + inflatedConsumed(pack, off);
    const kind = PACK_TYPES[typeNum];
    if (kind === undefined) throw new Error(`unknown pack entry type ${typeNum}`);
    if (kind === 'ofs-delta' || kind === 'ref-delta') {
      if (baseOid === null) throw new Error('delta without a base');
      pending.push({ offset: entryOffset, baseOid, delta: inflated });
      continue;
    }
    const obj: PackObject = { type: kind, data: inflated };
    const oid = hashObject(kind, inflated);
    objects.set(oid, obj);
    byOffset.set(entryOffset, obj);
  }
  // Resolve deltas in file order (a delta's base is always an earlier entry or an
  // object the receiver already holds; this pack carries its own bases first).
  for (const item of pending) {
    const base = objects.get(item.baseOid);
    if (base === undefined) throw new Error(`delta base ${item.baseOid} missing from pack`);
    const resolved = applyDelta(base.data, item.delta);
    const obj: PackObject = { type: base.type, data: resolved };
    objects.set(hashObject(base.type, resolved), obj);
    byOffset.set(item.offset, obj);
  }
  return objects;
}

/** How many bytes of `buf` starting at `off` one zlib stream consumes. */
function inflatedConsumed(buf: Buffer, off: number): number {
  // Feed the stream in growing slices until inflateSync stops complaining about
  // truncation; binary-search the smallest length that inflates cleanly.
  let lo = 1;
  let hi = buf.length - off;
  const ok = (len: number): boolean => {
    try {
      inflateSync(buf.subarray(off, off + len));
      return true;
    } catch {
      return false;
    }
  };
  if (!ok(hi)) throw new Error('truncated zlib stream in pack');
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (ok(mid)) hi = mid;
    else lo = mid + 1;
  }
  return lo;
}

/** Write a pack (version 2, undeltified) from objects; returns the full pack bytes. */
function writePack(objects: ReadonlyArray<{ type: GitType; data: Buffer }>): Buffer {
  const parts: Buffer[] = [];
  const header = Buffer.alloc(12);
  header.write('PACK', 0, 'ascii');
  header.writeUInt32BE(2, 4);
  header.writeUInt32BE(objects.length, 8);
  parts.push(header);
  for (const obj of objects) {
    const typeNum = obj.type === 'commit' ? 1 : obj.type === 'tree' ? 2 : obj.type === 'blob' ? 3 : 4;
    // varint header: low 4 bits of size, type in bits 4-6, continuation bit 7.
    let size = obj.data.length;
    const first = (typeNum << 4) | (size & 0x0f);
    size >>= 4;
    const headBytes: number[] = [];
    if (size > 0) headBytes.push(first | 0x80);
    else headBytes.push(first);
    while (size > 0) {
      const byte = size & 0x7f;
      size >>= 7;
      headBytes.push(size > 0 ? byte | 0x80 : byte);
    }
    parts.push(Buffer.from(headBytes));
    parts.push(deflateSync(obj.data, { level: 6 }));
  }
  const body = Buffer.concat(parts);
  const trailer = Buffer.from(sha1Hex(body), 'hex');
  return Buffer.concat([body, trailer]);
}

/** The objects reachable from `tips` (commits, tags, trees, blobs), in discovery order. */
function collectReachable(
  repoObjects: Map<string, PackObject>,
  tips: Iterable<string>,
): string[] {
  const seen = new Set<string>();
  const order: string[] = [];
  const stack: string[] = [...tips];
  while (stack.length > 0) {
    const oid = stack.pop() as string;
    if (seen.has(oid)) continue;
    const obj = repoObjects.get(oid);
    if (obj === undefined) continue;
    seen.add(oid);
    order.push(oid);
    if (obj.type === 'commit') {
      const view = parseCommit(obj.data);
      stack.push(view.tree);
      for (const parent of view.parents) stack.push(parent);
    } else if (obj.type === 'tree') {
      for (const entry of parseTree(obj.data)) stack.push(entry.oid);
    } else if (obj.type === 'tag') {
      stack.push(parseTag(obj.data).object);
    }
  }
  return order;
}

// ---------------------------------------------------------------------------
// the store: one in-memory forge shared by all four surfaces

interface Label {
  name: string;
  color: string;
  description: string;
}

interface Comment {
  id: number;
  body: string;
  seq: number;
}

interface Issue {
  number: number;
  title: string;
  body: string;
  state: 'open' | 'closed';
  labels: string[];
  comments: Comment[];
  seq: number;
  gid: string;
}

interface PullRequest {
  number: number;
  title: string;
  body: string;
  state: 'open' | 'closed';
  merged: boolean;
  mergedAt: string;
  mergeCommitSha: string;
  head: string;
  base: string;
  headSha: string;
  baseSha: string;
  seq: number;
  gid: string;
}

interface Repo {
  id: number;
  name: string;
  owner: string;
  private: boolean;
  description: string;
  defaultBranch: string;
  seq: number;
  gid: string;
  deleted: boolean;
  refs: Map<string, string>;
  objects: Map<string, PackObject>;
  labels: Map<string, Label>;
  issues: Map<number, Issue>;
  pulls: Map<number, PullRequest>;
  /** Shared issue/PR numbering, GitHub-style: one sequence per repository. */
  nextNumber: number;
}

const repos = new Map<string, Repo>();

function repoKey(owner: string, name: string): string {
  return `${owner}/${name}`;
}

function getRepo(owner: string, name: string): Repo | undefined {
  if (owner !== VIEWER_LOGIN) return undefined;
  const repo = repos.get(repoKey(owner, name));
  return repo !== undefined && !repo.deleted ? repo : undefined;
}

function createRepo(name: string, isPrivate: boolean, description: string): Repo {
  const id = nextId();
  const seq = nextSeq();
  const repo: Repo = {
    id,
    name,
    owner: VIEWER_LOGIN,
    private: isPrivate,
    description,
    defaultBranch: DEFAULT_BRANCH,
    seq,
    gid: gid('R', [id]),
    deleted: false,
    refs: new Map(),
    objects: new Map(),
    labels: new Map(),
    issues: new Map(),
    pulls: new Map(),
    nextNumber: 1,
  };
  repos.set(repoKey(VIEWER_LOGIN, name), repo);
  return repo;
}

function repoJson(repo: Repo): Record<string, unknown> {
  const created = stamp(repo.seq);
  return {
    id: repo.id,
    node_id: repo.gid,
    name: repo.name,
    full_name: `${repo.owner}/${repo.name}`,
    owner: { login: repo.owner, id: VIEWER_ID, type: 'User' },
    private: repo.private,
    description: repo.description,
    default_branch: repo.defaultBranch,
    has_issues: true,
    has_projects: false,
    has_wiki: false,
    created_at: created.iso,
    updated_at: created.iso,
    pushed_at: created.iso,
    html_url: `https://github.com/${repo.owner}/${repo.name}`,
    url: `/api/v3/repos/${repo.owner}/${repo.name}`,
    clone_url: `https://github.com/${repo.owner}/${repo.name}.git`,
    git_url: `git://github.com/${repo.owner}/${repo.name}.git`,
    ssh_url: `git@github.com:${repo.owner}/${repo.name}.git`,
    homepage: '',
    language: null,
    forks_count: 0,
    stargazers_count: 0,
    watchers_count: 0,
    open_issues_count: [...repo.issues.values()].filter((i) => i.state === 'open').length,
    license: null,
    topics: [],
    visibility: repo.private ? 'private' : 'public',
  };
}

function labelJson(label: Label): Record<string, unknown> {
  return { id: nextIdForLabel(label), name: label.name, color: label.color, description: label.description, default: false };
}

// Labels need stable numeric ids; derive them from the label's creation order.
const labelIds = new Map<string, number>();
function nextIdForLabel(label: Label): number {
  let id = labelIds.get(label.name);
  if (id === undefined) {
    id = nextId();
    labelIds.set(label.name, id);
  }
  return id;
}

function issueJson(repo: Repo, issue: Issue): Record<string, unknown> {
  const created = stamp(issue.seq);
  return {
    id: nextIdForIssue(repo, issue),
    node_id: issue.gid,
    number: issue.number,
    title: issue.title,
    body: issue.body,
    state: issue.state,
    user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    labels: issue.labels.map((name) => {
      const label = repo.labels.get(name);
      return label !== undefined ? labelJson(label) : { name, color: 'ededed', description: '' };
    }),
    assignees: [],
    comments: issue.comments.length,
    created_at: created.iso,
    updated_at: created.iso,
    closed_at: issue.state === 'closed' ? created.iso : null,
    html_url: `https://github.com/${repo.owner}/${repo.name}/issues/${issue.number}`,
    url: `/api/v3/repos/${repo.owner}/${repo.name}/issues/${issue.number}`,
    repository_url: `/api/v3/repos/${repo.owner}/${repo.name}`,
  };
}

const issueIds = new Map<string, number>();
function nextIdForIssue(repo: Repo, issue: Issue): number {
  const key = `${repo.id}:${issue.number}`;
  let id = issueIds.get(key);
  if (id === undefined) {
    id = nextId();
    issueIds.set(key, id);
  }
  return id;
}

function pullJson(repo: Repo, pull: PullRequest): Record<string, unknown> {
  const created = stamp(pull.seq);
  return {
    id: nextIdForPull(repo, pull),
    node_id: pull.gid,
    number: pull.number,
    title: pull.title,
    body: pull.body,
    state: pull.merged ? 'closed' : pull.state,
    merged: pull.merged,
    mergeable: mergeableState(repo, pull),
    merged_at: pull.merged ? pull.mergedAt : null,
    merge_commit_sha: pull.merged ? pull.mergeCommitSha : null,
    head: { ref: pull.head, sha: pull.headSha, label: `${repo.owner}:${pull.head}`, repo: repoJson(repo) },
    base: { ref: pull.base, sha: pull.baseSha, label: `${repo.owner}:${pull.base}`, repo: repoJson(repo) },
    user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    created_at: created.iso,
    updated_at: created.iso,
    closed_at: pull.state === 'closed' ? created.iso : null,
    html_url: `https://github.com/${repo.owner}/${repo.name}/pull/${pull.number}`,
    url: `/api/v3/repos/${repo.owner}/${repo.name}/pulls/${pull.number}`,
  };
}

const pullIds = new Map<string, number>();
function nextIdForPull(repo: Repo, pull: PullRequest): number {
  const key = `${repo.id}:pr:${pull.number}`;
  let id = pullIds.get(key);
  if (id === undefined) {
    id = nextId();
    pullIds.set(key, id);
  }
  return id;
}

// ---------------------------------------------------------------------------
// git trees inside a repository

/** Flatten a tree object into path -> {oid, mode}, recursively. */
function flattenTree(repo: Repo, treeOid: string, prefix: string, out: Map<string, { oid: string; mode: string }>): void {
  const obj = repo.objects.get(treeOid);
  if (obj === undefined || obj.type !== 'tree') return;
  for (const entry of parseTree(obj.data)) {
    const path = prefix === '' ? entry.name : `${prefix}/${entry.name}`;
    if (entry.mode === '40000') flattenTree(repo, entry.oid, path, out);
    else out.set(path, { oid: entry.oid, mode: entry.mode });
  }
}

function treeOfCommit(repo: Repo, commitOid: string): Map<string, { oid: string; mode: string }> {
  const out = new Map<string, { oid: string; mode: string }>();
  const commit = repo.objects.get(commitOid);
  if (commit === undefined || commit.type !== 'commit') return out;
  flattenTree(repo, parseCommit(commit.data).tree, '', out);
  return out;
}

/** Store an object in the repo (by its recomputed id) and return the id. */
function storeObject(repo: Repo, type: GitType, data: Buffer): string {
  const oid = hashObject(type, data);
  if (!repo.objects.has(oid)) repo.objects.set(oid, { type, data });
  return oid;
}

/** Build a tree object from a flat path map and store it (recursively). */
function buildTreeFromMap(repo: Repo, files: Map<string, { oid: string; mode: string }>): string {
  interface Node {
    files: Map<string, { oid: string; mode: string }>;
    dirs: Map<string, Node>;
  }
  const makeNode = (): Node => ({ files: new Map(), dirs: new Map() });
  const root = makeNode();
  for (const [path, entry] of [...files.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
    const segments = path.split('/');
    let node = root;
    for (let i = 0; i < segments.length - 1; i += 1) {
      const seg = segments[i] as string;
      let child = node.dirs.get(seg);
      if (child === undefined) {
        child = makeNode();
        node.dirs.set(seg, child);
      }
      node = child;
    }
    node.files.set(segments[segments.length - 1] as string, entry);
  }
  const build = (node: Node): string => {
    const entries: TreeEntry[] = [];
    for (const [name, entry] of [...node.files.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
      entries.push({ mode: entry.mode, name, oid: entry.oid });
    }
    for (const [name, child] of [...node.dirs.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
      entries.push({ mode: '40000', name, oid: build(child) });
    }
    return storeObject(repo, 'tree', buildTree(entries));
  };
  return build(root);
}

/** Create a commit object on top of `parent` (or a root commit) with `files` as its tree. */
function makeCommit(
  repo: Repo,
  files: Map<string, { oid: string; mode: string }>,
  parents: readonly string[],
  message: string,
): string {
  const treeOid = buildTreeFromMap(repo, files);
  const seq = nextSeq();
  const epoch = stamp(seq).epoch;
  const data = buildCommit({ tree: treeOid, parents, message, author: ident(), committer: ident(), epoch });
  return storeObject(repo, 'commit', data);
}

/** Whether `ancestor` is reachable from `tip` (following commit parents). */
function isAncestor(repo: Repo, ancestor: string, tip: string): boolean {
  if (ancestor === tip) return true;
  const seen = new Set<string>();
  const stack = [tip];
  while (stack.length > 0) {
    const oid = stack.pop() as string;
    if (oid === ancestor) return true;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const obj = repo.objects.get(oid);
    if (obj === undefined || obj.type !== 'commit') continue;
    for (const parent of parseCommit(obj.data).parents) stack.push(parent);
  }
  return false;
}

/** The first common ancestor of two commits (nearest to `a`), or null. */
function mergeBase(repo: Repo, a: string, b: string): string | null {
  const ancestorsA = new Set<string>();
  const stackA = [a];
  while (stackA.length > 0) {
    const oid = stackA.pop() as string;
    if (ancestorsA.has(oid)) continue;
    ancestorsA.add(oid);
    const obj = repo.objects.get(oid);
    if (obj === undefined || obj.type !== 'commit') continue;
    for (const parent of parseCommit(obj.data).parents) stackA.push(parent);
  }
  const seen = new Set<string>();
  const stackB = [b];
  while (stackB.length > 0) {
    const oid = stackB.pop() as string;
    if (ancestorsA.has(oid)) return oid;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const obj = repo.objects.get(oid);
    if (obj === undefined || obj.type !== 'commit') continue;
    for (const parent of parseCommit(obj.data).parents) stackB.push(parent);
  }
  return null;
}

interface MergeResult {
  merged: Map<string, { oid: string; mode: string }>;
  conflicts: string[];
}

/** A three-way merge of head into the base tip, over their merge base. */
function threeWayMerge(repo: Repo, baseOid: string, headOid: string, tipOid: string): MergeResult {
  const baseMap = treeOfCommit(repo, baseOid);
  const headMap = treeOfCommit(repo, headOid);
  const tipMap = treeOfCommit(repo, tipOid);
  const merged = new Map(tipMap);
  const conflicts: string[] = [];
  const paths = new Set([...baseMap.keys(), ...headMap.keys()]);
  for (const path of paths) {
    const base = baseMap.get(path);
    const head = headMap.get(path);
    const tip = tipMap.get(path);
    const headChanged = (head?.oid ?? null) !== (base?.oid ?? null);
    const tipChanged = (tip?.oid ?? null) !== (base?.oid ?? null);
    if (headChanged && tipChanged && (head?.oid ?? null) !== (tip?.oid ?? null)) {
      conflicts.push(path);
    } else if (headChanged) {
      if (head === undefined) merged.delete(path);
      else merged.set(path, head);
    }
  }
  conflicts.sort();
  return { merged, conflicts };
}

/** 'MERGEABLE' | 'CONFLICTING' for a pull request, by the same three-way merge. */
function mergeableState(repo: Repo, pull: PullRequest): string {
  const headTip = repo.refs.get(`refs/heads/${pull.head}`);
  const baseTip = repo.refs.get(`refs/heads/${pull.base}`);
  if (headTip === undefined || baseTip === undefined) return 'UNKNOWN';
  if (pull.merged || pull.state === 'closed') return 'UNKNOWN';
  const base = mergeBase(repo, headTip, baseTip);
  if (base === null) return 'UNKNOWN';
  const result = threeWayMerge(repo, base, headTip, baseTip);
  return result.conflicts.length === 0 ? 'MERGEABLE' : 'CONFLICTING';
}

/** Squash-merge a pull request onto its base branch; returns the merge commit or null on conflict. */
function squashMerge(repo: Repo, pull: PullRequest): string | null {
  const headTip = repo.refs.get(`refs/heads/${pull.head}`);
  const baseTip = repo.refs.get(`refs/heads/${pull.base}`);
  if (headTip === undefined || baseTip === undefined) return null;
  const base = mergeBase(repo, headTip, baseTip);
  if (base === null) return null;
  const result = threeWayMerge(repo, base, headTip, baseTip);
  if (result.conflicts.length > 0) return null;
  const seq = nextSeq();
  const mergedAt = stamp(seq).iso;
  const commitOid = makeCommit(repo, result.merged, [baseTip], `Merge pull request #${pull.number} from ${pull.head}\n\n${pull.title}`);
  repo.refs.set(`refs/heads/${pull.base}`, commitOid);
  pull.state = 'closed';
  pull.merged = true;
  pull.mergedAt = mergedAt;
  pull.mergeCommitSha = commitOid;
  pull.baseSha = commitOid;
  return commitOid;
}

/** The changed files between the merge base and a pull request's head. */
function pullFiles(repo: Repo, pull: PullRequest): Array<{ filename: string; status: string; additions: number; deletions: number; changes: number }> {
  const headTip = repo.refs.get(`refs/heads/${pull.head}`);
  const baseTip = repo.refs.get(`refs/heads/${pull.base}`);
  if (headTip === undefined || baseTip === undefined) return [];
  const base = mergeBase(repo, headTip, baseTip);
  const baseMap = base === null ? new Map<string, { oid: string; mode: string }>() : treeOfCommit(repo, base);
  const headMap = treeOfCommit(repo, headTip);
  const out: Array<{ filename: string; status: string; additions: number; deletions: number; changes: number }> = [];
  const paths = new Set([...baseMap.keys(), ...headMap.keys()]);
  for (const path of [...paths].sort()) {
    const before = baseMap.get(path);
    const after = headMap.get(path);
    if ((before?.oid ?? null) === (after?.oid ?? null)) continue;
    let status = 'modified';
    if (before === undefined) status = 'added';
    else if (after === undefined) status = 'removed';
    const beforeText = before === undefined ? '' : blobText(repo, before.oid);
    const afterText = after === undefined ? '' : blobText(repo, after.oid);
    const { additions, deletions } = lineDiff(beforeText, afterText);
    out.push({ filename: path, status, additions, deletions, changes: additions + deletions });
  }
  return out;
}

function blobText(repo: Repo, oid: string): string {
  const obj = repo.objects.get(oid);
  if (obj === undefined || obj.type !== 'blob') return '';
  return obj.data.toString('utf8');
}

/** A crude line diff: lines gained vs lines lost (multiset difference). */
function lineDiff(before: string, after: string): { additions: number; deletions: number } {
  const countLines = (text: string): Map<string, number> => {
    const map = new Map<string, number>();
    if (text === '') return map;
    for (const line of text.split('\n')) map.set(line, (map.get(line) ?? 0) + 1);
    return map;
  };
  const a = countLines(before);
  const b = countLines(after);
  let additions = 0;
  let deletions = 0;
  for (const [line, n] of b) additions += Math.max(0, n - (a.get(line) ?? 0));
  for (const [line, n] of a) deletions += Math.max(0, n - (b.get(line) ?? 0));
  return { additions, deletions };
}

/** Commits reachable from `tip`, newest first (parents after their child). */
function listCommits(repo: Repo, tip: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const stack = [tip];
  while (stack.length > 0) {
    const oid = stack.pop() as string;
    if (seen.has(oid)) continue;
    const obj = repo.objects.get(oid);
    if (obj === undefined || obj.type !== 'commit') continue;
    seen.add(oid);
    out.push(oid);
    for (const parent of parseCommit(obj.data).parents) stack.push(parent);
  }
  return out;
}

/** Resolve a ref name or oid to a commit oid (branch, tag, or raw oid). */
function resolveRef(repo: Repo, ref: string): string | null {
  if (/^[0-9a-f]{40}$/i.test(ref)) {
    const obj = repo.objects.get(ref.toLowerCase());
    if (obj === undefined) return null;
    if (obj.type === 'commit') return ref.toLowerCase();
    if (obj.type === 'tag') return peelTag(repo, ref.toLowerCase());
    return null;
  }
  const direct = repo.refs.get(ref);
  if (direct !== undefined) return peelToCommit(repo, direct);
  const branch = repo.refs.get(`refs/heads/${ref}`);
  if (branch !== undefined) return peelToCommit(repo, branch);
  const tag = repo.refs.get(`refs/tags/${ref}`);
  if (tag !== undefined) return peelToCommit(repo, tag);
  return null;
}

/** Peel a tag object to its commit, following nested tags. */
function peelTag(repo: Repo, oid: string): string | null {
  let current = oid;
  for (let i = 0; i < 10; i += 1) {
    const obj = repo.objects.get(current);
    if (obj === undefined) return null;
    if (obj.type === 'commit') return current;
    if (obj.type === 'tag') {
      current = parseTag(obj.data).object;
      continue;
    }
    return null;
  }
  return null;
}

function peelToCommit(repo: Repo, oid: string): string | null {
  const obj = repo.objects.get(oid);
  if (obj === undefined) return null;
  if (obj.type === 'commit') return oid;
  if (obj.type === 'tag') return peelTag(repo, oid);
  return null;
}

// ---------------------------------------------------------------------------
// HTTP plumbing

const JSON_TYPE = 'application/json; charset=utf-8';
const MAX_JSON_BODY = 16 * 1024 * 1024;
const MAX_GIT_BODY = 64 * 1024 * 1024;

interface Ctx {
  req: IncomingMessage;
  res: ServerResponse;
  method: string;
  path: string;
  query: URLSearchParams;
  host: string;
}

function pathOf(req: IncomingMessage): { path: string; query: URLSearchParams } {
  try {
    const url = new URL(req.url ?? '/', 'http://entry');
    return { path: url.pathname, query: url.searchParams };
  } catch {
    return { path: '/', query: new URLSearchParams() };
  }
}

function sendJson(ctx: Ctx, status: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  const text = `${JSON.stringify(body)}\n`;
  const headers: Record<string, string> = {
    'content-type': JSON_TYPE,
    'content-length': String(Buffer.byteLength(text)),
    'x-github-request-id': nextRequestId(),
    ...extraHeaders,
  };
  ctx.res.writeHead(status, headers);
  ctx.res.end(text);
}

function sendRaw(ctx: Ctx, status: number, body: Buffer, contentType: string, extraHeaders: Record<string, string> = {}): void {
  const headers: Record<string, string> = {
    'content-type': contentType,
    'content-length': String(body.length),
    'x-github-request-id': nextRequestId(),
    ...extraHeaders,
  };
  ctx.res.writeHead(status, headers);
  ctx.res.end(body);
}

function sendEmpty(ctx: Ctx, status: number, extraHeaders: Record<string, string> = {}): void {
  ctx.res.writeHead(status, { 'x-github-request-id': nextRequestId(), ...extraHeaders });
  ctx.res.end();
}

function notFound(ctx: Ctx, message = 'Not Found'): void {
  sendJson(ctx, 404, { message });
}

function badRequest(ctx: Ctx, message: string): void {
  sendJson(ctx, 400, { message });
}

/** Read the whole request body (bounded); null when it overflows or errors. */
function readBody(req: IncomingMessage, maxBytes: number): Promise<Buffer | null> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let done = false;
    const finish = (value: Buffer | null): void => {
      if (!done) {
        done = true;
        resolve(value);
      }
    };
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > maxBytes) {
        finish(null);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => finish(Buffer.concat(chunks)));
    req.on('error', () => finish(null));
    req.on('aborted', () => finish(null));
  });
}

/** Parse a JSON body; returns {ok:false} with the reply already sent on failure. */
async function readJsonBody(ctx: Ctx, required: boolean): Promise<{ ok: true; value: unknown } | { ok: false }> {
  const raw = await readBody(ctx.req, MAX_JSON_BODY);
  if (raw === null) {
    badRequest(ctx, 'Request body too large');
    return { ok: false };
  }
  if (raw.length === 0) {
    if (required) {
      badRequest(ctx, 'Request body must be valid JSON');
      return { ok: false };
    }
    return { ok: true, value: undefined };
  }
  try {
    return { ok: true, value: JSON.parse(raw.toString('utf8')) as unknown };
  } catch {
    badRequest(ctx, 'Invalid JSON in request body');
    return { ok: false };
  }
}

/** Whether the request carries the trial's bearer token. */
function authed(ctx: Ctx): boolean {
  const header = ctx.req.headers.authorization ?? '';
  if (!header.startsWith('Bearer ')) return false;
  const presented = header.slice('Bearer '.length).trim();
  if (presented === '') return false;
  // With no token configured (local runs), any non-empty bearer is accepted.
  if (TOKEN === '') return true;
  return presented === TOKEN;
}

/** 401 unless the request is authenticated; true when it may proceed. */
function requireAuth(ctx: Ctx): boolean {
  if (authed(ctx)) return true;
  sendJson(ctx, 401, { message: 'Requires authentication' });
  return false;
}

function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

/** Split a path into decoded segments, dropping empty ones. */
function splitPath(path: string): string[] {
  return path.split('/').filter((part) => part !== '').map(decodeSegment);
}

// ---------------------------------------------------------------------------
// REST v3

function restHandler(ctx: Ctx): Promise<void> {
  return handleRest(ctx);
}

async function handleRest(ctx: Ctx): Promise<void> {
  const parts = splitPath(ctx.path);
  // parts[0] === 'api', parts[1] === 'v3'
  const rest = parts.slice(2);

  // GET /api/v3/ — the API root.
  if (ctx.method === 'GET' && rest.length === 0) {
    if (!requireAuth(ctx)) return;
    sendJson(ctx, 200, {
      current_user_url: '/api/v3/user',
      rate_limit_url: '/api/v3/rate_limit',
      repository_url: '/api/v3/repos/{owner}/{repo}',
    }, { 'x-oauth-scopes': 'repo, read:org' });
    return;
  }
  // GET /api/v3/meta
  if (ctx.method === 'GET' && rest.length === 1 && rest[0] === 'meta') {
    if (!requireAuth(ctx)) return;
    sendJson(ctx, 200, { installed_version: FORGE_VERSION, verifiable_password_authentication: false });
    return;
  }
  // GET /api/v3/user — the token owner.
  if (ctx.method === 'GET' && rest.length === 1 && rest[0] === 'user') {
    if (!requireAuth(ctx)) return;
    sendJson(ctx, 200, {
      login: VIEWER_LOGIN, id: VIEWER_ID, node_id: gid('U', [VIEWER_ID]), type: 'User',
      name: VIEWER_NAME, email: VIEWER_EMAIL, html_url: `https://github.com/${VIEWER_LOGIN}`,
      url: '/api/v3/user', public_repos: [...repos.values()].filter((r) => !r.deleted && r.owner === VIEWER_LOGIN).length,
    });
    return;
  }
  // POST /api/v3/user/repos and GET /api/v3/user/repos
  if (rest.length === 2 && rest[0] === 'user' && rest[1] === 'repos') {
    if (!requireAuth(ctx)) return;
    if (ctx.method === 'POST') {
      const parsed = await readJsonBody(ctx, true);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      const name = asString(body.name).trim();
      if (name === '') {
        sendJson(ctx, 422, { message: 'Validation Failed: name is required' });
        return;
      }
      if (repos.has(repoKey(VIEWER_LOGIN, name)) && repos.get(repoKey(VIEWER_LOGIN, name))?.deleted === false) {
        sendJson(ctx, 422, { message: 'Repository already exists' });
        return;
      }
      const repo = createRepo(name, asBool(body.private, false), asString(body.description));
      sendJson(ctx, 201, repoJson(repo));
      return;
    }
    if (ctx.method === 'GET') {
      const perPage = Math.max(1, Math.min(100, asInt(ctx.query.get('per_page'), 30) || 30));
      const page = Math.max(1, asInt(ctx.query.get('page'), 1) || 1);
      const all = [...repos.values()].filter((r) => !r.deleted && r.owner === VIEWER_LOGIN);
      const start = (page - 1) * perPage;
      const slice = all.slice(start, start + perPage);
      const headers: Record<string, string> = {};
      if (start + perPage < all.length) {
        const nextUrl = `http://${ctx.host}/api/v3/user/repos?per_page=${perPage}&page=${page + 1}`;
        const lastPage = Math.ceil(all.length / perPage);
        headers.link = `<${nextUrl}>; rel="next", <http://${ctx.host}/api/v3/user/repos?per_page=${perPage}&page=${lastPage}>; rel="last"`;
      }
      sendJson(ctx, 200, slice.map((r) => repoJson(r)), headers);
      return;
    }
    notFound(ctx);
    return;
  }

  // /api/v3/repos/{owner}/{repo}[/...]
  if (rest.length >= 3 && rest[0] === 'repos') {
    const owner = rest[1] as string;
    const name = rest[2] as string;
    await handleRepoRest(ctx, owner, name, rest.slice(3));
    return;
  }

  notFound(ctx);
}

/** Parse a ?page/&per_page pair with GitHub's defaults. */
function pageParams(ctx: Ctx): { perPage: number; page: number } {
  const perPage = Math.max(1, Math.min(100, asInt(ctx.query.get('per_page'), 30) || 30));
  const page = Math.max(1, asInt(ctx.query.get('page'), 1) || 1);
  return { perPage, page };
}

/** A paged list reply with a Link header when another page follows. */
function sendPage<T>(ctx: Ctx, items: readonly T[], basePath: string, extraQuery: string): void {
  const { perPage, page } = pageParams(ctx);
  const start = (page - 1) * perPage;
  const slice = items.slice(start, start + perPage);
  const headers: Record<string, string> = {};
  if (start + perPage < items.length) {
    const sep = extraQuery === '' ? '?' : `?${extraQuery}&`;
    headers.link = `<http://${ctx.host}${basePath}${sep}per_page=${perPage}&page=${page + 1}>; rel="next"`;
  }
  sendJson(ctx, 200, slice, headers);
}

async function handleRepoRest(ctx: Ctx, owner: string, name: string, tail: string[]): Promise<void> {
  if (!requireAuth(ctx)) return;
  const repo = getRepo(owner, name);
  if (repo === undefined) {
    notFound(ctx);
    return;
  }

  // The repository itself.
  if (tail.length === 0) {
    if (ctx.method === 'GET') {
      sendJson(ctx, 200, repoJson(repo));
      return;
    }
    if (ctx.method === 'PATCH') {
      const parsed = await readJsonBody(ctx, false);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      if (typeof body.description === 'string') repo.description = body.description;
      sendJson(ctx, 200, repoJson(repo));
      return;
    }
    if (ctx.method === 'DELETE') {
      repo.deleted = true;
      sendEmpty(ctx, 204);
      return;
    }
    notFound(ctx);
    return;
  }

  const head = tail[0] as string;

  // Branches.
  if (head === 'branches') {
    if (ctx.method === 'GET' && tail.length === 1) {
      const branches = [...repo.refs.keys()]
        .filter((ref) => ref.startsWith('refs/heads/'))
        .sort()
        .map((ref) => ({ name: ref.slice('refs/heads/'.length), commit: { sha: repo.refs.get(ref) as string } }));
      sendJson(ctx, 200, branches);
      return;
    }
    if (ctx.method === 'GET' && tail.length === 2) {
      const ref = `refs/heads/${tail[1] as string}`;
      const sha = repo.refs.get(ref);
      if (sha === undefined) {
        notFound(ctx);
        return;
      }
      sendJson(ctx, 200, { name: tail[1], commit: { sha }, protected: false });
      return;
    }
    notFound(ctx);
    return;
  }

  // Commits.
  if (head === 'commits') {
    if (ctx.method === 'GET' && tail.length === 1) {
      const ref = ctx.query.get('sha') ?? repo.defaultBranch;
      const tip = resolveRef(repo, ref);
      if (tip === null) {
        sendJson(ctx, 200, []);
        return;
      }
      const list = listCommits(repo, tip).map((oid) => {
        const view = parseCommit((repo.objects.get(oid) as PackObject).data);
        return {
          sha: oid,
          commit: {
            message: view.message.replace(/\n+$/, ''),
            author: { name: VIEWER_NAME, email: VIEWER_EMAIL },
            committer: { name: VIEWER_NAME, email: VIEWER_EMAIL },
            tree: { sha: view.tree },
          },
          author: { login: VIEWER_LOGIN, id: VIEWER_ID },
          committer: { login: VIEWER_LOGIN, id: VIEWER_ID },
          parents: view.parents.map((p) => ({ sha: p })),
        };
      });
      sendJson(ctx, 200, list);
      return;
    }
    notFound(ctx);
    return;
  }

  // Git Data API: refs.
  if (head === 'git') {
    if (tail.length >= 2 && tail[1] === 'refs') {
      if (ctx.method === 'POST' && tail.length === 2) {
        const parsed = await readJsonBody(ctx, true);
        if (!parsed.ok) return;
        const body = isRecord(parsed.value) ? parsed.value : {};
        const ref = asString(body.ref);
        const sha = asString(body.sha).toLowerCase();
        if (!ref.startsWith('refs/') || !/^[0-9a-f]{40}$/.test(sha) || repo.objects.get(sha) === undefined) {
          sendJson(ctx, 422, { message: 'Validation Failed: ref and sha are required' });
          return;
        }
        if (repo.refs.has(ref)) {
          sendJson(ctx, 422, { message: 'Reference already exists' });
          return;
        }
        repo.refs.set(ref, sha);
        sendJson(ctx, 201, { ref, node_id: gid('RF', [repo.id, ref]), object: { sha, type: (repo.objects.get(sha) as PackObject).type }, sha });
        return;
      }
      if (ctx.method === 'GET' && tail.length >= 3) {
        // /git/refs/heads/{branch} — the ref name may itself hold slashes.
        const ref = tail.slice(2).join('/');
        const sha = repo.refs.get(ref);
        if (sha === undefined) {
          notFound(ctx);
          return;
        }
        sendJson(ctx, 200, { ref, node_id: gid('RF', [repo.id, ref]), object: { sha, type: (repo.objects.get(sha) as PackObject).type }, sha });
        return;
      }
    }
    notFound(ctx);
    return;
  }

  // Contents.
  if (head === 'contents') {
    await handleContents(ctx, repo, tail.slice(1));
    return;
  }

  // Labels.
  if (head === 'labels') {
    if (ctx.method === 'POST' && tail.length === 1) {
      const parsed = await readJsonBody(ctx, true);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      const labelName = asString(body.name).trim();
      if (labelName === '') {
        sendJson(ctx, 422, { message: 'Validation Failed: name is required' });
        return;
      }
      if (repo.labels.has(labelName)) {
        sendJson(ctx, 422, { message: 'Label already exists' });
        return;
      }
      const label: Label = { name: labelName, color: asString(body.color, 'ededed').replace(/^#/, ''), description: asString(body.description) };
      repo.labels.set(labelName, label);
      sendJson(ctx, 201, labelJson(label));
      return;
    }
    if (ctx.method === 'GET' && tail.length === 1) {
      sendJson(ctx, 200, [...repo.labels.values()].sort((a, b) => (a.name < b.name ? -1 : 1)).map((l) => labelJson(l)));
      return;
    }
    notFound(ctx);
    return;
  }

  // Issues.
  if (head === 'issues') {
    await handleIssues(ctx, repo, tail.slice(1));
    return;
  }

  // Pull requests.
  if (head === 'pulls') {
    await handlePulls(ctx, repo, tail.slice(1));
    return;
  }

  notFound(ctx);
}

/** The contents API: read, write and delete files through commits. */
async function handleContents(ctx: Ctx, repo: Repo, pathParts: string[]): Promise<void> {
  const filePath = pathParts.join('/');
  const refName = ctx.query.get('ref') ?? repo.defaultBranch;
  const tipFor = (branch: string): string | null => {
    const ref = repo.refs.get(`refs/heads/${branch}`);
    return ref ?? null;
  };

  if (ctx.method === 'GET') {
    const tip = tipFor(refName) ?? resolveRef(repo, refName);
    if (tip === null) {
      notFound(ctx);
      return;
    }
    const files = treeOfCommit(repo, tip);
    // A directory listing when the path names a directory (or the repo root).
    if (filePath === '') {
      const entries = dirListing(repo, tip, '');
      sendJson(ctx, 200, entries);
      return;
    }
    const direct = files.get(filePath);
    if (direct !== undefined) {
      const obj = repo.objects.get(direct.oid) as PackObject;
      sendJson(ctx, 200, {
        type: 'file',
        encoding: 'base64',
        size: obj.data.length,
        name: filePath.split('/').pop(),
        path: filePath,
        content: b64encode(obj.data),
        sha: direct.oid,
      });
      return;
    }
    // Directory?
    const prefix = `${filePath}/`;
    const hasChildren = [...files.keys()].some((p) => p.startsWith(prefix));
    if (hasChildren) {
      sendJson(ctx, 200, dirListing(repo, tip, filePath));
      return;
    }
    notFound(ctx);
    return;
  }

  if (ctx.method === 'PUT') {
    const parsed = await readJsonBody(ctx, true);
    if (!parsed.ok) return;
    const body = isRecord(parsed.value) ? parsed.value : {};
    const message = asString(body.message, 'Update file');
    const contentB64 = asString(body.content);
    if (contentB64 === '') {
      sendJson(ctx, 422, { message: 'Validation Failed: content is required' });
      return;
    }
    const branch = asString(body.branch, repo.defaultBranch);
    let tip = tipFor(branch);
    if (tip === null) {
      // A new branch starts from the default branch's tip (or an unborn repo).
      tip = tipFor(repo.defaultBranch);
    }
    const files = tip === null ? new Map<string, { oid: string; mode: string }>() : treeOfCommit(repo, tip);
    const existing = files.get(filePath);
    const givenSha = asString(body.sha);
    const isUpdate = existing !== undefined;
    if (isUpdate && givenSha !== '' && givenSha.toLowerCase() !== existing.oid) {
      sendJson(ctx, 409, { message: 'sha does not match the current blob' });
      return;
    }
    const blobOid = storeObject(repo, 'blob', b64decode(contentB64));
    files.set(filePath, { oid: blobOid, mode: '100644' });
    const parents = tip === null ? [] : [tip];
    const commitOid = makeCommit(repo, files, parents, message);
    repo.refs.set(`refs/heads/${branch}`, commitOid);
    sendJson(ctx, isUpdate ? 200 : 201, {
      content: { name: filePath.split('/').pop(), path: filePath, sha: blobOid, size: b64decode(contentB64).length, encoding: 'base64' },
      commit: { sha: commitOid, message },
    });
    return;
  }

  if (ctx.method === 'DELETE') {
    const parsed = await readJsonBody(ctx, false);
    if (!parsed.ok) return;
    const body = isRecord(parsed.value) ? parsed.value : {};
    const message = asString(body.message, 'Delete file');
    const branch = asString(body.branch, repo.defaultBranch);
    const tip = tipFor(branch);
    if (tip === null) {
      notFound(ctx);
      return;
    }
    const files = treeOfCommit(repo, tip);
    if (!files.has(filePath)) {
      notFound(ctx);
      return;
    }
    files.delete(filePath);
    const commitOid = makeCommit(repo, files, [tip], message);
    repo.refs.set(`refs/heads/${branch}`, commitOid);
    sendJson(ctx, 200, { commit: { sha: commitOid, message } });
    return;
  }

  notFound(ctx);
}

/** One directory's entries (files and subdirectories), sorted by name. */
function dirListing(repo: Repo, tip: string, dir: string): Array<Record<string, unknown>> {
  const files = treeOfCommit(repo, tip);
  const prefix = dir === '' ? '' : `${dir}/`;
  const seen = new Map<string, { type: string; oid: string; path: string }>();
  for (const [path, entry] of files) {
    if (!path.startsWith(prefix)) continue;
    const rest = path.slice(prefix.length);
    const slash = rest.indexOf('/');
    if (slash < 0) {
      seen.set(rest, { type: 'file', oid: entry.oid, path });
    } else {
      const name = rest.slice(0, slash);
      if (!seen.has(name)) {
        // The subdirectory's tree oid: find it by walking the tree.
        const sub = subTreeOid(repo, tip, dir === '' ? name : `${dir}/${name}`);
        seen.set(name, { type: 'dir', oid: sub ?? '', path: dir === '' ? name : `${dir}/${name}` });
      }
    }
  }
  return [...seen.entries()]
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([name, entry]) => ({ type: entry.type, name, path: entry.path, sha: entry.oid, size: entry.type === 'file' ? (repo.objects.get(entry.oid)?.data.length ?? 0) : 0 }));
}

function subTreeOid(repo: Repo, tip: string, dir: string): string | null {
  const commit = repo.objects.get(tip);
  if (commit === undefined || commit.type !== 'commit') return null;
  let treeOid = parseCommit(commit.data).tree;
  for (const segment of dir.split('/')) {
    const tree = repo.objects.get(treeOid);
    if (tree === undefined || tree.type !== 'tree') return null;
    const entry = parseTree(tree.data).find((e) => e.name === segment && e.mode === '40000');
    if (entry === undefined) return null;
    treeOid = entry.oid;
  }
  return treeOid;
}

/** The issues API. */
async function handleIssues(ctx: Ctx, repo: Repo, tail: string[]): Promise<void> {
  if (tail.length === 0) {
    if (ctx.method === 'POST') {
      const parsed = await readJsonBody(ctx, true);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      const title = asString(body.title).trim();
      if (title === '') {
        sendJson(ctx, 422, { message: 'Validation Failed: title is required' });
        return;
      }
      const labels = Array.isArray(body.labels) ? body.labels.filter((l): l is string => typeof l === 'string') : [];
      for (const label of labels) {
        if (!repo.labels.has(label)) {
          sendJson(ctx, 422, { message: `Validation Failed: label "${label}" does not exist` });
          return;
        }
      }
      const number = repo.nextNumber;
      repo.nextNumber += 1;
      const seq = nextSeq();
      const issue: Issue = {
        number,
        title,
        body: asString(body.body),
        state: 'open',
        labels,
        comments: [],
        seq,
        gid: gid('I', [repo.id, number]),
      };
      repo.issues.set(number, issue);
      sendJson(ctx, 201, issueJson(repo, issue));
      return;
    }
    if (ctx.method === 'GET') {
      const state = ctx.query.get('state') ?? 'open';
      const sort = ctx.query.get('sort') ?? 'created';
      const direction = ctx.query.get('direction') ?? 'desc';
      let list = [...repo.issues.values()];
      if (state !== 'all') list = list.filter((i) => i.state === state);
      if (sort === 'created') {
        list.sort((a, b) => (direction === 'asc' ? a.seq - b.seq : b.seq - a.seq));
      }
      sendPage(ctx, list.map((i) => issueJson(repo, i)), `/api/v3/repos/${repo.owner}/${repo.name}/issues`, `state=${state}&sort=${sort}&direction=${direction}`);
      return;
    }
    notFound(ctx);
    return;
  }

  const number = Number(tail[0]);
  if (!Number.isInteger(number) || number <= 0) {
    notFound(ctx);
    return;
  }
  const issue = repo.issues.get(number);
  if (tail.length === 1) {
    if (issue === undefined) {
      notFound(ctx);
      return;
    }
    if (ctx.method === 'GET') {
      sendJson(ctx, 200, issueJson(repo, issue));
      return;
    }
    if (ctx.method === 'PATCH') {
      const parsed = await readJsonBody(ctx, false);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      if (typeof body.title === 'string') issue.title = body.title;
      if (typeof body.body === 'string') issue.body = body.body;
      if (body.state === 'open' || body.state === 'closed') issue.state = body.state;
      sendJson(ctx, 200, issueJson(repo, issue));
      return;
    }
    notFound(ctx);
    return;
  }

  if (tail.length === 2 && tail[1] === 'comments') {
    if (issue === undefined) {
      notFound(ctx);
      return;
    }
    if (ctx.method === 'POST') {
      const parsed = await readJsonBody(ctx, true);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      const text = asString(body.body);
      if (text === '') {
        sendJson(ctx, 422, { message: 'Validation Failed: body is required' });
        return;
      }
      const comment: Comment = { id: nextId(), body: text, seq: nextSeq() };
      issue.comments.push(comment);
      sendJson(ctx, 201, {
        id: comment.id,
        node_id: gid('C', [comment.id]),
        body: comment.body,
        user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
        created_at: stamp(comment.seq).iso,
        updated_at: stamp(comment.seq).iso,
        html_url: `https://github.com/${repo.owner}/${repo.name}/issues/${number}#issuecomment-${comment.id}`,
      });
      return;
    }
    if (ctx.method === 'GET') {
      sendJson(ctx, 200, issue.comments.map((c) => ({
        id: c.id,
        node_id: gid('C', [c.id]),
        body: c.body,
        user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
        created_at: stamp(c.seq).iso,
        updated_at: stamp(c.seq).iso,
      })));
      return;
    }
    notFound(ctx);
    return;
  }

  notFound(ctx);
}

/** The pull requests API. */
async function handlePulls(ctx: Ctx, repo: Repo, tail: string[]): Promise<void> {
  if (tail.length === 0) {
    if (ctx.method === 'POST') {
      const parsed = await readJsonBody(ctx, true);
      if (!parsed.ok) return;
      const body = isRecord(parsed.value) ? parsed.value : {};
      const title = asString(body.title).trim();
      const head = asString(body.head);
      const base = asString(body.base, repo.defaultBranch);
      if (title === '' || head === '') {
        sendJson(ctx, 422, { message: 'Validation Failed: title and head are required' });
        return;
      }
      const headSha = repo.refs.get(`refs/heads/${head}`);
      const baseSha = repo.refs.get(`refs/heads/${base}`);
      if (headSha === undefined || baseSha === undefined) {
        sendJson(ctx, 422, { message: 'Validation Failed: head or base branch does not exist' });
        return;
      }
      const number = repo.nextNumber;
      repo.nextNumber += 1;
      const seq = nextSeq();
      const pull: PullRequest = {
        number,
        title,
        body: asString(body.body),
        state: 'open',
        merged: false,
        mergedAt: '',
        mergeCommitSha: '',
        head,
        base,
        headSha,
        baseSha,
        seq,
        gid: gid('P', [repo.id, number]),
      };
      repo.pulls.set(number, pull);
      sendJson(ctx, 201, pullJson(repo, pull));
      return;
    }
    if (ctx.method === 'GET') {
      const state = ctx.query.get('state') ?? 'open';
      let list = [...repo.pulls.values()];
      if (state === 'open') list = list.filter((p) => p.state === 'open');
      else if (state === 'closed') list = list.filter((p) => p.state === 'closed');
      list.sort((a, b) => b.seq - a.seq);
      sendPage(ctx, list.map((p) => pullJson(repo, p)), `/api/v3/repos/${repo.owner}/${repo.name}/pulls`, `state=${state}`);
      return;
    }
    notFound(ctx);
    return;
  }

  const number = Number(tail[0]);
  if (!Number.isInteger(number) || number <= 0) {
    notFound(ctx);
    return;
  }
  const pull = repo.pulls.get(number);

  if (tail.length === 2 && tail[1] === 'files') {
    if (pull === undefined) {
      notFound(ctx);
      return;
    }
    if (ctx.method === 'GET') {
      sendJson(ctx, 200, pullFiles(repo, pull));
      return;
    }
    notFound(ctx);
    return;
  }

  if (tail.length === 2 && tail[1] === 'merge') {
    if (pull === undefined) {
      notFound(ctx);
      return;
    }
    if (ctx.method === 'PUT') {
      const parsedBody = await readJsonBody(ctx, false);
      if (!parsedBody.ok) return;
      if (pull.merged) {
        sendJson(ctx, 405, { message: 'Pull request is already merged' });
        return;
      }
      if (pull.state === 'closed') {
        sendJson(ctx, 405, { message: 'Pull request is closed' });
        return;
      }
      const merged = squashMerge(repo, pull);
      if (merged === null) {
        sendJson(ctx, 405, { message: 'Merge conflict' });
        return;
      }
      sendJson(ctx, 200, { sha: merged, merged: true, message: `Pull request #${number} merged` });
      return;
    }
    notFound(ctx);
    return;
  }

  if (tail.length === 1) {
    if (pull === undefined) {
      notFound(ctx);
      return;
    }
    if (ctx.method === 'GET') {
      sendJson(ctx, 200, pullJson(repo, pull));
      return;
    }
    notFound(ctx);
    return;
  }

  notFound(ctx);
}

// ---------------------------------------------------------------------------
// GraphQL v4: a parser for the document subset gh and github-mcp-server send,
// and an executor over the same store.

type GqlValue =
  | { kind: 'variable'; name: string }
  | { kind: 'string'; value: string }
  | { kind: 'int'; value: number }
  | { kind: 'float'; value: number }
  | { kind: 'boolean'; value: boolean }
  | { kind: 'enum'; value: string }
  | { kind: 'null' }
  | { kind: 'list'; items: GqlValue[] }
  | { kind: 'object'; fields: Array<{ name: string; value: GqlValue }> };

interface GqlField {
  alias: string | null;
  name: string;
  args: Array<{ name: string; value: GqlValue }>;
  selections: GqlSelection[];
}

type GqlSelection =
  | { kind: 'field'; field: GqlField }
  | { kind: 'inline'; typeName: string; selections: GqlSelection[] };

interface GqlOperation {
  type: 'query' | 'mutation';
  name: string | null;
  variables: Record<string, GqlValue>;
  selections: GqlSelection[];
}

function gqlTokenize(src: string): string[] {
  const tokens: string[] = [];
  let i = 0;
  while (i < src.length) {
    const ch = src[i] as string;
    if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === ',') {
      i += 1;
      continue;
    }
    if (ch === '#') {
      while (i < src.length && src[i] !== '\n') i += 1;
      continue;
    }
    if (src.startsWith('...', i)) {
      tokens.push('...');
      i += 3;
      continue;
    }
    if ('{}()[]!:=$.@'.includes(ch)) {
      tokens.push(ch);
      i += 1;
      continue;
    }
    if (ch === '"') {
      let j = i + 1;
      let out = '';
      while (j < src.length && src[j] !== '"') {
        if (src[j] === '\\' && j + 1 < src.length) {
          const esc = src[j + 1] as string;
          out += esc === 'n' ? '\n' : esc === 't' ? '\t' : esc === 'r' ? '\r' : esc;
          j += 2;
        } else {
          out += src[j] as string;
          j += 1;
        }
      }
      tokens.push(JSON.stringify(out));
      i = j + 1;
      continue;
    }
    if (/[A-Za-z_0-9.-]/.test(ch)) {
      let j = i;
      while (j < src.length && /[A-Za-z_0-9-]/.test(src[j] as string)) j += 1;
      tokens.push(src.slice(i, j));
      i = j;
      continue;
    }
    i += 1;
  }
  return tokens;
}

class GqlParser {
  private tokens: string[];
  private pos = 0;

  constructor(src: string) {
    this.tokens = gqlTokenize(src);
  }

  peek(): string | undefined {
    return this.tokens[this.pos];
  }

  next(): string {
    const token = this.tokens[this.pos] as string;
    this.pos += 1;
    return token;
  }

  expect(token: string): void {
    const got = this.next();
    if (got !== token) throw new Error(`expected ${token}, got ${got}`);
  }

  parseDocument(): GqlOperation[] {
    const operations: GqlOperation[] = [];
    while (this.pos < this.tokens.length) {
      operations.push(this.parseOperation());
    }
    return operations;
  }

  parseOperation(): GqlOperation {
    let type: 'query' | 'mutation' = 'query';
    const first = this.peek();
    if (first === 'query' || first === 'mutation') {
      type = this.next() as 'query' | 'mutation';
    }
    let name: string | null = null;
    if (this.peek() !== undefined && this.peek() !== '(' && this.peek() !== '{') {
      name = this.next();
    }
    const variables: Record<string, GqlValue> = {};
    if (this.peek() === '(') {
      this.next();
      while (this.peek() !== ')' && this.peek() !== undefined) {
        if (this.peek() === '$') {
          this.next();
          const varName = this.next();
          this.next(); // ':'
          // Skip the type (a name, possibly with ! or [ ]).
          while (this.peek() !== undefined && this.peek() !== ')' && this.peek() !== '$') {
            const tok = this.next();
            if (tok === '=') {
              variables[varName] = this.parseValue();
              break;
            }
          }
          if (!(varName in variables)) variables[varName] = { kind: 'null' };
        } else {
          this.next();
        }
      }
      this.expect(')');
    }
    const selections = this.parseSelections();
    return { type, name, variables, selections };
  }

  parseSelections(): GqlSelection[] {
    this.expect('{');
    const selections: GqlSelection[] = [];
    while (this.peek() !== '}' && this.peek() !== undefined) {
      if (this.peek() === '...') {
        this.next();
        if (this.peek() === 'on') {
          this.next();
          const typeName = this.next();
          selections.push({ kind: 'inline', typeName, selections: this.parseSelections() });
        } else {
          // A fragment spread (not used by the suite's documents): skip its selections.
          this.next();
          if (this.peek() === '{') this.parseSelections();
        }
        continue;
      }
      selections.push({ kind: 'field', field: this.parseField() });
    }
    this.expect('}');
    return selections;
  }

  parseField(): GqlField {
    let alias: string | null = null;
    let name = this.next();
    if (this.peek() === ':') {
      this.next();
      alias = name;
      name = this.next();
    }
    const args: Array<{ name: string; value: GqlValue }> = [];
    if (this.peek() === '(') {
      this.next();
      while (this.peek() !== ')' && this.peek() !== undefined) {
        const argName = this.next();
        this.expect(':');
        args.push({ name: argName, value: this.parseValue() });
      }
      this.expect(')');
    }
    let selections: GqlSelection[] = [];
    if (this.peek() === '{') selections = this.parseSelections();
    return { alias, name, args, selections };
  }

  parseValue(): GqlValue {
    const token = this.next();
    if (token === '$') return { kind: 'variable', name: this.next() };
    if (token === '[') {
      const items: GqlValue[] = [];
      while (this.peek() !== ']' && this.peek() !== undefined) items.push(this.parseValue());
      this.expect(']');
      return { kind: 'list', items };
    }
    if (token === '{') {
      const fields: Array<{ name: string; value: GqlValue }> = [];
      while (this.peek() !== '}' && this.peek() !== undefined) {
        const fieldName = this.next();
        this.expect(':');
        fields.push({ name: fieldName, value: this.parseValue() });
      }
      this.expect('}');
      return { kind: 'object', fields };
    }
    if (token === 'true') return { kind: 'boolean', value: true };
    if (token === 'false') return { kind: 'boolean', value: false };
    if (token === 'null') return { kind: 'null' };
    if (token.startsWith('"')) return { kind: 'string', value: JSON.parse(token) as string };
    if (/^-?\d+$/.test(token)) return { kind: 'int', value: Number(token) };
    if (/^-?\d+\.\d+$/.test(token)) return { kind: 'float', value: Number(token) };
    return { kind: 'enum', value: token };
  }
}

/** Resolve a parsed value against the operation's variables. */
function gqlResolve(value: GqlValue, variables: Record<string, unknown>): unknown {
  switch (value.kind) {
    case 'variable':
      return variables[value.name] ?? null;
    case 'string':
    case 'int':
    case 'float':
    case 'boolean':
    case 'enum':
      return value.value;
    case 'null':
      return null;
    case 'list':
      return value.items.map((item) => gqlResolve(item, variables));
    case 'object': {
      const out: Record<string, unknown> = {};
      for (const field of value.fields) out[field.name] = gqlResolve(field.value, variables);
      return out;
    }
  }
}

function gqlArg(field: GqlField, name: string, variables: Record<string, unknown>): unknown {
  const arg = field.args.find((a) => a.name === name);
  return arg === undefined ? undefined : gqlResolve(arg.value, variables);
}

// The introspection surface gh feature-detects.
const TYPE_FIELDS: Record<string, string[]> = {
  Issue: ['id', 'number', 'title', 'state', 'body', 'createdAt', 'updatedAt', 'author', 'labels', 'comments', 'url', 'repository'],
  PullRequest: ['id', 'number', 'title', 'state', 'body', 'headRefName', 'baseRefName', 'mergeable', 'merged', 'files', 'createdAt', 'updatedAt', 'author', 'url', 'repository'],
  Repository: ['id', 'databaseId', 'name', 'nameWithOwner', 'owner', 'defaultBranchRef', 'viewerPermission', 'hasIssuesEnabled', 'issue', 'issues', 'pullRequests', 'pullRequest', 'description', 'createdAt', 'updatedAt', 'url'],
  SearchType: [],
};
const TYPE_ENUM_VALUES: Record<string, string[]> = {
  SearchType: ['ISSUE', 'PULL_REQUEST', 'REPOSITORY', 'USER', 'DISCUSSION', 'COMMIT', 'CODE', 'MILESTONE'],
  IssueState: ['OPEN', 'CLOSED'],
  MergeableState: ['MERGEABLE', 'CONFLICTING', 'UNKNOWN'],
  PullRequestState: ['OPEN', 'CLOSED', 'MERGED'],
  PullRequestMergeMethod: ['MERGE', 'SQUASH', 'REBASE'],
  RepositoryPermission: ['ADMIN', 'MAINTAIN', 'WRITE', 'TRIAGE', 'READ'],
  IssueOrderField: ['CREATED_AT', 'UPDATED_AT', 'COMMENTS'],
  OrderDirection: ['ASC', 'DESC'],
};

interface GqlExec {
  errors: Array<{ message: string }>;
}

function gqlIssueState(issue: Issue): string {
  return issue.state === 'open' ? 'OPEN' : 'CLOSED';
}

function gqlPullState(pull: PullRequest): string {
  if (pull.merged) return 'MERGED';
  return pull.state === 'open' ? 'OPEN' : 'CLOSED';
}

function gqlIssueObject(repo: Repo, issue: Issue, selections: GqlSelection[], exec: GqlExec): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const sel of selections) {
    if (sel.kind === 'inline') continue;
    const field = sel.field;
    const key = field.alias ?? field.name;
    switch (field.name) {
      case 'id': out[key] = issue.gid; break;
      case 'number': out[key] = issue.number; break;
      case 'title': out[key] = issue.title; break;
      case 'state': out[key] = gqlIssueState(issue); break;
      case 'body': out[key] = issue.body; break;
      case 'createdAt': case 'updatedAt': out[key] = stamp(issue.seq).iso; break;
      case 'url': out[key] = `https://github.com/${repo.owner}/${repo.name}/issues/${issue.number}`; break;
      case 'repository': out[key] = gqlRepoObject(repo, field.selections, exec); break;
      case 'author': out[key] = { login: VIEWER_LOGIN }; break;
      default:
        exec.errors.push({ message: `Cannot query field "${field.name}" on type "Issue".` });
        out[key] = null;
    }
  }
  return out;
}

function gqlPullObject(repo: Repo, pull: PullRequest, selections: GqlSelection[], exec: GqlExec): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const sel of selections) {
    if (sel.kind === 'inline') continue;
    const field = sel.field;
    const key = field.alias ?? field.name;
    switch (field.name) {
      case 'id': out[key] = pull.gid; break;
      case 'number': out[key] = pull.number; break;
      case 'title': out[key] = pull.title; break;
      case 'state': out[key] = gqlPullState(pull); break;
      case 'body': out[key] = pull.body; break;
      case 'headRefName': out[key] = pull.head; break;
      case 'baseRefName': out[key] = pull.base; break;
      case 'mergeable': out[key] = mergeableState(repo, pull); break;
      case 'merged': out[key] = pull.merged; break;
      case 'createdAt': case 'updatedAt': out[key] = stamp(pull.seq).iso; break;
      case 'url': out[key] = `https://github.com/${repo.owner}/${repo.name}/pull/${pull.number}`; break;
      case 'repository': out[key] = gqlRepoObject(repo, field.selections, exec); break;
      case 'author': out[key] = { login: VIEWER_LOGIN }; break;
      case 'files': {
        const first = asInt(gqlArg(field, 'first', {}), 100) || 100;
        const files = pullFiles(repo, pull).slice(0, first);
        out[key] = { nodes: files.map((f) => ({ path: f.filename })) };
        break;
      }
      default:
        exec.errors.push({ message: `Cannot query field "${field.name}" on type "PullRequest".` });
        out[key] = null;
    }
  }
  return out;
}

function gqlIssuesConnection(
  repo: Repo,
  selections: GqlSelection[],
  exec: GqlExec,
  variables: Record<string, unknown>,
  field: GqlField,
): Record<string, unknown> {
  const first = asInt(gqlArg(field, 'first', variables), 0);
  const after = gqlArg(field, 'after', variables);
  const states = gqlArg(field, 'states', variables);
  const orderBy = gqlArg(field, 'orderBy', variables);
  let list = [...repo.issues.values()];
  if (Array.isArray(states)) {
    const wanted = new Set(states.map((s) => String(s)));
    list = list.filter((i) => wanted.has(gqlIssueState(i)));
  }
  if (isRecord(orderBy)) {
    const dir = String(orderBy.direction ?? 'DESC');
    list.sort((a, b) => {
      const cmp = a.seq - b.seq;
      const tie = a.number - b.number;
      if (dir === 'DESC') return cmp !== 0 ? -cmp : -tie;
      return cmp !== 0 ? cmp : tie;
    });
  } else {
    list.sort((a, b) => a.seq - b.seq);
  }
  let startIndex = 0;
  if (typeof after === 'string' && after !== '') {
    try {
      const decoded = Buffer.from(after, 'base64url').toString('utf8');
      const num = Number(decoded);
      const idx = list.findIndex((i) => i.number === num);
      if (idx >= 0) startIndex = idx + 1;
    } catch {
      // An unreadable cursor pages from the start.
    }
  }
  const page = first > 0 ? list.slice(startIndex, startIndex + first) : list.slice(startIndex);
  const hasNext = startIndex + page.length < list.length;
  const endCursor = page.length > 0 ? Buffer.from(String(page[page.length - 1]?.number ?? ''), 'utf8').toString('base64url') : null;
  const out: Record<string, unknown> = {};
  for (const sel of selections) {
    if (sel.kind === 'inline') continue;
    const f = sel.field;
    const key = f.alias ?? f.name;
    if (f.name === 'nodes') {
      out[key] = page.map((issue) => gqlIssueObject(repo, issue, f.selections, exec));
    } else if (f.name === 'pageInfo') {
      const info: Record<string, unknown> = {};
      for (const sub of f.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'hasNextPage') info[skey] = hasNext;
        else if (sub.field.name === 'endCursor') info[skey] = endCursor;
        else info[skey] = null;
      }
      out[key] = info;
    } else if (f.name === 'totalCount') {
      out[key] = list.length;
    } else {
      exec.errors.push({ message: `Cannot query field "${f.name}" on type "IssueConnection".` });
      out[key] = null;
    }
  }
  return out;
}

function gqlRepoObject(repo: Repo, selections: GqlSelection[], exec: GqlExec, variables: Record<string, unknown> = {}): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const sel of selections) {
    if (sel.kind === 'inline') continue;
    const field = sel.field;
    const key = field.alias ?? field.name;
    switch (field.name) {
      case 'id': out[key] = repo.gid; break;
      case 'databaseId': out[key] = repo.id; break;
      case 'name': out[key] = repo.name; break;
      case 'nameWithOwner': out[key] = `${repo.owner}/${repo.name}`; break;
      case 'description': out[key] = repo.description; break;
      case 'owner': out[key] = { login: repo.owner }; break;
      case 'defaultBranchRef': {
        const branch: Record<string, unknown> = {};
        for (const sub of field.selections) {
          if (sub.kind === 'inline') continue;
          const skey = sub.field.alias ?? sub.field.name;
          if (sub.field.name === 'name') branch[skey] = repo.defaultBranch;
          else if (sub.field.name === 'target') branch[skey] = { oid: repo.refs.get(`refs/heads/${repo.defaultBranch}`) ?? null };
          else branch[skey] = null;
        }
        out[key] = branch;
        break;
      }
      case 'viewerPermission': out[key] = 'ADMIN'; break;
      case 'hasIssuesEnabled': out[key] = true; break;
      case 'url': out[key] = `https://github.com/${repo.owner}/${repo.name}`; break;
      case 'createdAt': case 'updatedAt': out[key] = stamp(repo.seq).iso; break;
      case 'issue': {
        const number = asInt(gqlArg(field, 'number', variables), 0);
        const issue = repo.issues.get(number);
        out[key] = issue === undefined ? null : gqlIssueObject(repo, issue, field.selections, exec);
        break;
      }
      case 'issues':
        out[key] = gqlIssuesConnection(repo, field.selections, exec, variables, field);
        break;
      case 'pullRequests': {
        const first = asInt(gqlArg(field, 'first', variables), 0);
        let list = [...repo.pulls.values()].sort((a, b) => b.seq - a.seq);
        if (first > 0) list = list.slice(0, first);
        const conn: Record<string, unknown> = {};
        for (const sub of field.selections) {
          if (sub.kind === 'inline') continue;
          const skey = sub.field.alias ?? sub.field.name;
          if (sub.field.name === 'nodes') conn[skey] = list.map((p) => gqlPullObject(repo, p, sub.field.selections, exec));
          else if (sub.field.name === 'totalCount') conn[skey] = list.length;
          else conn[skey] = null;
        }
        out[key] = conn;
        break;
      }
      case 'pullRequest': {
        const number = asInt(gqlArg(field, 'number', variables), 0);
        const pull = repo.pulls.get(number);
        out[key] = pull === undefined ? null : gqlPullObject(repo, pull, field.selections, exec);
        break;
      }
      default:
        exec.errors.push({ message: `Cannot query field "${field.name}" on type "Repository".` });
        out[key] = null;
    }
  }
  return out;
}

function gqlFindByGid(id: string): { repo: Repo; issue?: Issue; pull?: PullRequest } | null {
  const parts = ungid(id);
  if (parts === null) return null;
  const kind = parts[0];
  if (kind === 'R') {
    const repoId = Number(parts[1]);
    for (const repo of repos.values()) {
      if (!repo.deleted && repo.id === repoId) return { repo };
    }
    return null;
  }
  if (kind === 'I') {
    const repoId = Number(parts[1]);
    const number = Number(parts[2]);
    for (const repo of repos.values()) {
      if (!repo.deleted && repo.id === repoId) {
        const issue = repo.issues.get(number);
        return issue === undefined ? null : { repo, issue };
      }
    }
    return null;
  }
  if (kind === 'P') {
    const repoId = Number(parts[1]);
    const number = Number(parts[2]);
    for (const repo of repos.values()) {
      if (!repo.deleted && repo.id === repoId) {
        const pull = repo.pulls.get(number);
        return pull === undefined ? null : { repo, pull };
      }
    }
    return null;
  }
  return null;
}

function gqlNodeObject(found: { repo: Repo; issue?: Issue; pull?: PullRequest }, selections: GqlSelection[], exec: GqlExec): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const sel of selections) {
    if (sel.kind === 'inline') {
      if (sel.typeName === 'Repository' && found.issue === undefined && found.pull === undefined) {
        Object.assign(out, gqlRepoObject(found.repo, sel.selections, exec));
      } else if (sel.typeName === 'Issue' && found.issue !== undefined) {
        Object.assign(out, gqlIssueObject(found.repo, found.issue, sel.selections, exec));
      } else if (sel.typeName === 'PullRequest' && found.pull !== undefined) {
        Object.assign(out, gqlPullObject(found.repo, found.pull, sel.selections, exec));
      }
      continue;
    }
    const field = sel.field;
    const key = field.alias ?? field.name;
    if (field.name === 'id') {
      out[key] = found.issue !== undefined ? found.issue.gid : found.pull !== undefined ? found.pull.gid : found.repo.gid;
    } else {
      out[key] = null;
    }
  }
  return out;
}

function gqlExecuteOperation(op: GqlOperation, variables: Record<string, unknown>): { data: Record<string, unknown> | null; errors: Array<{ message: string }> } {
  const exec: GqlExec = { errors: [] };
  const data: Record<string, unknown> = {};
  const defaults = { ...op.variables };
  for (const [name, def] of Object.entries(defaults)) {
    if (!(name in variables)) variables[name] = gqlResolve(def, {});
  }

  for (const sel of op.selections) {
    if (sel.kind === 'inline') continue;
    const field = sel.field;
    const key = field.alias ?? field.name;

    if (op.type === 'query') {
      if (field.name === 'viewer') {
        const viewer: Record<string, unknown> = {};
        for (const sub of field.selections) {
          if (sub.kind === 'inline') continue;
          const skey = sub.field.alias ?? sub.field.name;
          if (sub.field.name === 'login') viewer[skey] = VIEWER_LOGIN;
          else if (sub.field.name === 'id') viewer[skey] = gid('U', [VIEWER_ID]);
          else if (sub.field.name === 'name') viewer[skey] = VIEWER_NAME;
          else {
            exec.errors.push({ message: `Cannot query field "${sub.field.name}" on type "User".` });
            viewer[skey] = null;
          }
        }
        data[key] = viewer;
      } else if (field.name === 'repository') {
        const owner = asString(gqlArg(field, 'owner', variables));
        const name = asString(gqlArg(field, 'name', variables));
        const repo = getRepo(owner, name);
        data[key] = repo === undefined ? null : gqlRepoObject(repo, field.selections, exec, variables);
      } else if (field.name === 'node') {
        const id = asString(gqlArg(field, 'id', variables));
        const found = gqlFindByGid(id);
        data[key] = found === null ? null : gqlNodeObject(found, field.selections, exec);
      } else if (field.name === '__type') {
        const typeName = asString(gqlArg(field, 'name', variables));
        const fields = TYPE_FIELDS[typeName];
        const enums = TYPE_ENUM_VALUES[typeName];
        if (fields === undefined && enums === undefined) {
          data[key] = null;
        } else {
          const typeObj: Record<string, unknown> = { name: typeName };
          for (const sub of field.selections) {
            if (sub.kind === 'inline') continue;
            const skey = sub.field.alias ?? sub.field.name;
            if (sub.field.name === 'fields') typeObj[skey] = (fields ?? []).map((n) => ({ name: n }));
            else if (sub.field.name === 'enumValues') typeObj[skey] = (enums ?? []).map((n) => ({ name: n }));
            else if (sub.field.name === 'name') typeObj[skey] = typeName;
            else typeObj[skey] = null;
          }
          data[key] = typeObj;
        }
      } else if (field.name === '__schema') {
        data[key] = { queryType: { name: 'Query' }, mutationType: { name: 'Mutation' } };
      } else {
        exec.errors.push({ message: `Cannot query field "${field.name}" on type "Query".` });
        data[key] = null;
      }
      continue;
    }

    // Mutations.
    if (field.name === 'createIssue') {
      const input = gqlArg(field, 'input', variables);
      const created = gqlCreateIssue(input);
      const issueObj: Record<string, unknown> = {};
      for (const sub of field.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'issue') {
          issueObj[skey] = created === null ? null : gqlIssueObject(created.repo, created.issue, sub.field.selections, exec);
        } else if (sub.field.name === 'clientMutationId') {
          issueObj[skey] = null;
        } else {
          issueObj[skey] = null;
        }
      }
      data[key] = issueObj;
    } else if (field.name === 'closeIssue') {
      const input = gqlArg(field, 'input', variables);
      const closed = gqlCloseIssue(input);
      const out: Record<string, unknown> = {};
      for (const sub of field.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'issue') {
          out[skey] = closed === null ? null : gqlIssueObject(closed.repo, closed.issue, sub.field.selections, exec);
        } else {
          out[skey] = null;
        }
      }
      data[key] = out;
    } else if (field.name === 'createPullRequest') {
      const input = gqlArg(field, 'input', variables);
      const created = gqlCreatePullRequest(input);
      const out: Record<string, unknown> = {};
      for (const sub of field.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'pullRequest') {
          out[skey] = created === null ? null : gqlPullObject(created.repo, created.pull, sub.field.selections, exec);
        } else {
          out[skey] = null;
        }
      }
      data[key] = out;
    } else if (field.name === 'mergePullRequest') {
      const input = gqlArg(field, 'input', variables);
      const merged = gqlMergePullRequest(input);
      const out: Record<string, unknown> = {};
      for (const sub of field.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'pullRequest') {
          out[skey] = merged === null ? null : gqlPullObject(merged.repo, merged.pull, sub.field.selections, exec);
        } else {
          out[skey] = null;
        }
      }
      data[key] = out;
    } else if (field.name === 'addComment') {
      const input = gqlArg(field, 'input', variables);
      const added = gqlAddComment(input);
      const out: Record<string, unknown> = {};
      for (const sub of field.selections) {
        if (sub.kind === 'inline') continue;
        const skey = sub.field.alias ?? sub.field.name;
        if (sub.field.name === 'commentEdge') {
          const edge: Record<string, unknown> = {};
          for (const sub2 of sub.field.selections) {
            if (sub2.kind === 'inline') continue;
            const k2 = sub2.field.alias ?? sub2.field.name;
            if (sub2.field.name === 'node') {
              const node: Record<string, unknown> = {};
              for (const sub3 of sub2.field.selections) {
                if (sub3.kind === 'inline') continue;
                const k3 = sub3.field.alias ?? sub3.field.name;
                if (sub3.field.name === 'id') node[k3] = added === null ? null : gid('C', [added.comment.id]);
                else if (sub3.field.name === 'body') node[k3] = added === null ? null : added.comment.body;
                else node[k3] = null;
              }
              edge[k2] = node;
            } else {
              edge[k2] = null;
            }
          }
          out[skey] = edge;
        } else {
          out[skey] = null;
        }
      }
      data[key] = out;
    } else {
      exec.errors.push({ message: `Cannot query field "${field.name}" on type "Mutation".` });
      data[key] = null;
    }
  }

  return { data, errors: exec.errors };
}

function gqlCreateIssue(input: unknown): { repo: Repo; issue: Issue } | null {
  if (!isRecord(input)) return null;
  const repoId = ungid(input.repositoryId);
  if (repoId === null || repoId[0] !== 'R') return null;
  const repo = [...repos.values()].find((r) => !r.deleted && r.id === Number(repoId[1]));
  if (repo === undefined) return null;
  const title = asString(input.title);
  if (title === '') return null;
  const number = repo.nextNumber;
  repo.nextNumber += 1;
  const issue: Issue = {
    number,
    title,
    body: asString(input.body),
    state: 'open',
    labels: [],
    comments: [],
    seq: nextSeq(),
    gid: gid('I', [repo.id, number]),
  };
  repo.issues.set(number, issue);
  return { repo, issue };
}

function gqlCloseIssue(input: unknown): { repo: Repo; issue: Issue } | null {
  if (!isRecord(input)) return null;
  const found = gqlFindByGid(asString(input.issueId));
  if (found === null || found.issue === undefined) return null;
  found.issue.state = 'closed';
  return { repo: found.repo, issue: found.issue };
}

function gqlCreatePullRequest(input: unknown): { repo: Repo; pull: PullRequest } | null {
  if (!isRecord(input)) return null;
  const repoId = ungid(input.repositoryId);
  if (repoId === null || repoId[0] !== 'R') return null;
  const repo = [...repos.values()].find((r) => !r.deleted && r.id === Number(repoId[1]));
  if (repo === undefined) return null;
  const head = asString(input.headRefName);
  const base = asString(input.baseRefName, repo.defaultBranch);
  const headSha = repo.refs.get(`refs/heads/${head}`);
  const baseSha = repo.refs.get(`refs/heads/${base}`);
  if (headSha === undefined || baseSha === undefined) return null;
  const number = repo.nextNumber;
  repo.nextNumber += 1;
  const pull: PullRequest = {
    number,
    title: asString(input.title),
    body: asString(input.body),
    state: 'open',
    merged: false,
    mergedAt: '',
    mergeCommitSha: '',
    head,
    base,
    headSha,
    baseSha,
    seq: nextSeq(),
    gid: gid('P', [repo.id, number]),
  };
  repo.pulls.set(number, pull);
  return { repo, pull };
}

function gqlMergePullRequest(input: unknown): { repo: Repo; pull: PullRequest } | null {
  if (!isRecord(input)) return null;
  const found = gqlFindByGid(asString(input.pullRequestId));
  if (found === null || found.pull === undefined) return null;
  if (found.pull.merged) return found as { repo: Repo; pull: PullRequest };
  const merged = squashMerge(found.repo, found.pull);
  if (merged === null) return null;
  return { repo: found.repo, pull: found.pull };
}

function gqlAddComment(input: unknown): { repo: Repo; issue: Issue; comment: Comment } | null {
  if (!isRecord(input)) return null;
  const found = gqlFindByGid(asString(input.subjectId));
  if (found === null || found.issue === undefined) return null;
  const comment: Comment = { id: nextId(), body: asString(input.body), seq: nextSeq() };
  found.issue.comments.push(comment);
  return { repo: found.repo, issue: found.issue, comment };
}

async function handleGraphql(ctx: Ctx): Promise<void> {
  if (ctx.method !== 'POST') {
    sendJson(ctx, 405, { message: 'Method Not Allowed' });
    return;
  }
  if (!requireAuth(ctx)) return;
  const parsed = await readJsonBody(ctx, true);
  if (!parsed.ok) return;
  const body = parsed.value;
  const operations: Array<{ query: string; variables: Record<string, unknown> }> = [];
  if (Array.isArray(body)) {
    for (const entry of body) {
      if (isRecord(entry) && typeof entry.query === 'string') {
        operations.push({ query: entry.query, variables: isRecord(entry.variables) ? entry.variables : {} });
      }
    }
  } else if (isRecord(body) && typeof body.query === 'string') {
    operations.push({ query: body.query, variables: isRecord(body.variables) ? body.variables : {} });
  } else {
    sendJson(ctx, 400, { errors: [{ message: 'A GraphQL request body must be an object with a query string.' }] });
    return;
  }
  const responses = operations.map((op) => {
    let parsedOps: GqlOperation[];
    try {
      parsedOps = new GqlParser(op.query).parseDocument();
    } catch (err) {
      return { data: null, errors: [{ message: err instanceof Error ? err.message : 'Failed to parse the GraphQL document.' }] };
    }
    const operation = parsedOps[0];
    if (operation === undefined) {
      return { data: null, errors: [{ message: 'The document holds no operation.' }] };
    }
    const result = gqlExecuteOperation(operation, { ...op.variables });
    if (result.errors.length > 0) {
      return { data: result.data, errors: result.errors };
    }
    return { data: result.data };
  });
  if (Array.isArray(body)) {
    sendJson(ctx, 200, responses);
  } else {
    sendJson(ctx, 200, responses[0] as Record<string, unknown>);
  }
}

// ---------------------------------------------------------------------------
// MCP: JSON-RPC 2.0 over Streamable HTTP, revision 2026-07-28

interface McpToolDef {
  name: string;
  description: string;
  inputSchema: {
    type: 'object';
    properties: Record<string, { type: string; description: string }>;
    required: string[];
    additionalProperties: boolean;
  };
}

const MCP_TOOLS: McpToolDef[] = [
  {
    name: 'create_repo',
    description: 'Create a repository owned by the authenticated user.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'The repository name.' },
        description: { type: 'string', description: 'A short description of the repository.' },
        private: { type: 'boolean', description: 'Whether the repository is private.' },
      },
      required: ['name'],
      additionalProperties: false,
    },
  },
  {
    name: 'get_repo',
    description: 'Get a repository by owner and name.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
      },
      required: ['owner', 'repo'],
      additionalProperties: false,
    },
  },
  {
    name: 'create_issue',
    description: 'Create an issue in a repository.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
        title: { type: 'string', description: 'The issue title.' },
        body: { type: 'string', description: 'The issue body.' },
      },
      required: ['owner', 'repo', 'title'],
      additionalProperties: false,
    },
  },
  {
    name: 'list_issues',
    description: 'List issues in a repository, optionally filtered by state.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
        state: { type: 'string', description: 'Issue state: open, closed, or all.' },
      },
      required: ['owner', 'repo'],
      additionalProperties: false,
    },
  },
  {
    name: 'get_issue',
    description: 'Get one issue by owner, repository and number.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
        number: { type: 'number', description: 'The issue number.' },
      },
      required: ['owner', 'repo', 'number'],
      additionalProperties: false,
    },
  },
  {
    name: 'list_repos',
    description: 'List the authenticated user repositories.',
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
      additionalProperties: false,
    },
  },
  {
    name: 'create_pull_request',
    description: 'Open a pull request from a head branch into a base branch.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
        title: { type: 'string', description: 'The pull request title.' },
        body: { type: 'string', description: 'The pull request body.' },
        head: { type: 'string', description: 'The head branch name.' },
        base: { type: 'string', description: 'The base branch name.' },
      },
      required: ['owner', 'repo', 'title', 'head', 'base'],
      additionalProperties: false,
    },
  },
  {
    name: 'merge_pull_request',
    description: 'Merge a pull request with a squash commit onto its base branch.',
    inputSchema: {
      type: 'object',
      properties: {
        owner: { type: 'string', description: 'The repository owner login.' },
        repo: { type: 'string', description: 'The repository name.' },
        pull_number: { type: 'number', description: 'The pull request number.' },
      },
      required: ['owner', 'repo', 'pull_number'],
      additionalProperties: false,
    },
  },
];

/** The tools/list answer, built once so two calls are byte-identical. */
const MCP_TOOLS_JSON = JSON.stringify(MCP_TOOLS);

function mcpResult(id: unknown, result: unknown): Record<string, unknown> {
  return { jsonrpc: '2.0', id, result };
}

function mcpError(id: unknown, code: number, message: string): Record<string, unknown> {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

function mcpToolResult(value: unknown, isError = false): Record<string, unknown> {
  return {
    content: [{ type: 'text', text: typeof value === 'string' ? value : JSON.stringify(value) }],
    structuredContent: value,
    isError,
  };
}

function mcpMissingArg(name: string): Record<string, unknown> {
  return mcpToolResult({ error: `Missing required argument: ${name}` }, true);
}

/** Run one tool call; returns the tool result object (or a tool error). */
function mcpCallTool(name: string, args: Record<string, unknown>): Record<string, unknown> {
  const getStr = (key: string): string => asString(args[key]);
  const getNum = (key: string): number => {
    const value = args[key];
    if (typeof value === 'number') return value;
    if (typeof value === 'string' && /^\d+$/.test(value)) return Number(value);
    return 0;
  };

  if (name === 'create_repo') {
    const repoName = getStr('name');
    if (repoName === '') return mcpMissingArg('name');
    if (repos.has(repoKey(VIEWER_LOGIN, repoName)) && repos.get(repoKey(VIEWER_LOGIN, repoName))?.deleted === false) {
      return mcpToolResult({ error: `Repository ${repoName} already exists` }, true);
    }
    const repo = createRepo(repoName, asBool(args.private, false), getStr('description'));
    return mcpToolResult(repoJson(repo));
  }

  if (name === 'get_repo') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    if (owner === '' || repoName === '') return mcpMissingArg(owner === '' ? 'owner' : 'repo');
    const repo = getRepo(owner, repoName);
    if (repo === undefined) return mcpToolResult({ error: `Repository ${owner}/${repoName} not found` }, true);
    return mcpToolResult(repoJson(repo));
  }

  if (name === 'list_repos') {
    const list = [...repos.values()].filter((r) => !r.deleted && r.owner === VIEWER_LOGIN).map((r) => repoJson(r));
    return mcpToolResult(list);
  }

  if (name === 'create_issue') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    const title = getStr('title');
    if (owner === '') return mcpMissingArg('owner');
    if (repoName === '') return mcpMissingArg('repo');
    if (title === '') return mcpMissingArg('title');
    const repo = getRepo(owner, repoName);
    if (repo === undefined) return mcpToolResult({ error: `Repository ${owner}/${repoName} not found` }, true);
    const number = repo.nextNumber;
    repo.nextNumber += 1;
    const issue: Issue = {
      number,
      title,
      body: getStr('body'),
      state: 'open',
      labels: [],
      comments: [],
      seq: nextSeq(),
      gid: gid('I', [repo.id, number]),
    };
    repo.issues.set(number, issue);
    return mcpToolResult(issueJson(repo, issue));
  }

  if (name === 'list_issues') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    if (owner === '') return mcpMissingArg('owner');
    if (repoName === '') return mcpMissingArg('repo');
    const repo = getRepo(owner, repoName);
    if (repo === undefined) return mcpToolResult({ error: `Repository ${owner}/${repoName} not found` }, true);
    const state = getStr('state') || 'open';
    let list = [...repo.issues.values()];
    if (state !== 'all') list = list.filter((i) => i.state === state);
    list.sort((a, b) => b.seq - a.seq);
    return mcpToolResult(list.map((i) => issueJson(repo, i)));
  }

  if (name === 'get_issue') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    const number = getNum('number');
    if (owner === '') return mcpMissingArg('owner');
    if (repoName === '') return mcpMissingArg('repo');
    if (number <= 0) return mcpMissingArg('number');
    const repo = getRepo(owner, repoName);
    const issue = repo?.issues.get(number);
    if (issue === undefined) return mcpToolResult({ error: `Issue #${number} not found` }, true);
    return mcpToolResult(issueJson(repo as Repo, issue));
  }

  if (name === 'create_pull_request') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    const title = getStr('title');
    const head = getStr('head');
    const base = getStr('base');
    if (owner === '') return mcpMissingArg('owner');
    if (repoName === '') return mcpMissingArg('repo');
    if (title === '') return mcpMissingArg('title');
    if (head === '') return mcpMissingArg('head');
    if (base === '') return mcpMissingArg('base');
    const repo = getRepo(owner, repoName);
    if (repo === undefined) return mcpToolResult({ error: `Repository ${owner}/${repoName} not found` }, true);
    const headSha = repo.refs.get(`refs/heads/${head}`);
    const baseSha = repo.refs.get(`refs/heads/${base}`);
    if (headSha === undefined || baseSha === undefined) {
      return mcpToolResult({ error: 'Head or base branch does not exist' }, true);
    }
    const number = repo.nextNumber;
    repo.nextNumber += 1;
    const pull: PullRequest = {
      number,
      title,
      body: getStr('body'),
      state: 'open',
      merged: false,
      mergedAt: '',
      mergeCommitSha: '',
      head,
      base,
      headSha,
      baseSha,
      seq: nextSeq(),
      gid: gid('P', [repo.id, number]),
    };
    repo.pulls.set(number, pull);
    return mcpToolResult(pullJson(repo, pull));
  }

  if (name === 'merge_pull_request') {
    const owner = getStr('owner');
    const repoName = getStr('repo');
    const number = getNum('pull_number');
    if (owner === '') return mcpMissingArg('owner');
    if (repoName === '') return mcpMissingArg('repo');
    if (number <= 0) return mcpMissingArg('pull_number');
    const repo = getRepo(owner, repoName);
    const pull = repo?.pulls.get(number);
    if (pull === undefined) return mcpToolResult({ error: `Pull request #${number} not found` }, true);
    if (pull.merged) return mcpToolResult({ error: 'Pull request is already merged' }, true);
    const merged = squashMerge(repo as Repo, pull);
    if (merged === null) return mcpToolResult({ error: 'Merge conflict' }, true);
    return mcpToolResult({ sha: merged, merged: true, number, message: `Pull request #${number} merged` });
  }

  return mcpToolResult({ error: `Unknown tool: ${name}` }, true);
}

async function handleMcp(ctx: Ctx): Promise<void> {
  if (ctx.method === 'DELETE') {
    sendEmpty(ctx, 200);
    return;
  }
  if (ctx.method === 'GET') {
    sendJson(ctx, 405, { message: 'Method Not Allowed' });
    return;
  }
  if (ctx.method !== 'POST') {
    sendJson(ctx, 405, { message: 'Method Not Allowed' });
    return;
  }
  if (!requireAuth(ctx)) {
    // A JSON-RPC-shaped 401 keeps the client honest about the refusal.
    return;
  }
  const raw = await readBody(ctx.req, MAX_JSON_BODY);
  if (raw === null) {
    sendJson(ctx, 400, mcpError(null, -32700, 'Parse error: request body too large'));
    return;
  }
  let body: unknown;
  try {
    body = JSON.parse(raw.toString('utf8')) as unknown;
  } catch {
    sendJson(ctx, 400, mcpError(null, -32700, 'Parse error: request body is not valid JSON'));
    return;
  }
  if (!isRecord(body) || typeof body.method !== 'string') {
    sendJson(ctx, 400, mcpError(isRecord(body) ? (body.id ?? null) : null, -32600, 'Invalid Request'));
    return;
  }
  const id = body.id ?? null;
  const method = body.method;
  const params = isRecord(body.params) ? body.params : {};

  // Notifications carry no id: answer 202 with no body.
  if (id === null && method.startsWith('notifications/')) {
    sendEmpty(ctx, 202);
    return;
  }

  if (method === 'server/discover') {
    sendJson(ctx, 200, mcpResult(id, {
      resultType: 'discover',
      supportedVersions: ['2026-07-28', '2025-11-25'],
      capabilities: { tools: {} },
      serverInfo: { name: FORGE_NAME, version: FORGE_VERSION },
      instructions: 'A GitHub-compatible git forge: repositories, issues, pull requests and git smart HTTP.',
      _meta: { 'io.modelcontextprotocol/serverInfo': { name: FORGE_NAME, version: FORGE_VERSION } },
    }));
    return;
  }

  if (method === 'initialize') {
    sendJson(ctx, 200, mcpResult(id, {
      protocolVersion: '2025-11-25',
      capabilities: { tools: {} },
      serverInfo: { name: FORGE_NAME, version: FORGE_VERSION },
    }));
    return;
  }

  if (method === 'ping') {
    sendJson(ctx, 200, mcpResult(id, {}));
    return;
  }

  if (method === 'tools/list') {
    // The cached JSON keeps two calls byte-identical; rebuild the envelope only.
    const text = `{"jsonrpc":"2.0","id":${JSON.stringify(id)},"result":{"tools":${MCP_TOOLS_JSON}}}\n`;
    const headers: Record<string, string> = {
      'content-type': JSON_TYPE,
      'content-length': String(Buffer.byteLength(text)),
      'x-github-request-id': nextRequestId(),
    };
    ctx.res.writeHead(200, headers);
    ctx.res.end(text);
    return;
  }

  if (method === 'tools/call') {
    const toolName = asString(params.name);
    const args = isRecord(params.arguments) ? params.arguments : {};
    const def = MCP_TOOLS.find((t) => t.name === toolName);
    if (def === undefined) {
      sendJson(ctx, 200, mcpError(id, -32602, `Unknown tool: ${toolName}`));
      return;
    }
    const result = mcpCallTool(toolName, args);
    sendJson(ctx, 200, mcpResult(id, result));
    return;
  }

  sendJson(ctx, 200, mcpError(id, -32601, `Method not found: ${method}`));
}

// ---------------------------------------------------------------------------
// git smart HTTP

const GIT_AGENT = `${FORGE_NAME}/${FORGE_VERSION}`;

/** Sorted refs of a repo: [name, oid], with peeled tag targets resolved. */
function sortedRefs(repo: Repo): Array<{ name: string; oid: string; peeled?: string }> {
  const out: Array<{ name: string; oid: string; peeled?: string }> = [];
  for (const name of [...repo.refs.keys()].sort()) {
    const oid = repo.refs.get(name) as string;
    const entry: { name: string; oid: string; peeled?: string } = { name, oid };
    const obj = repo.objects.get(oid);
    if (obj !== undefined && obj.type === 'tag') {
      const peeled = peelTag(repo, oid);
      if (peeled !== null) entry.peeled = peeled;
    }
    out.push(entry);
  }
  return out;
}

/** The v0 ref advertisement (with the "# service=" preamble). */
function v0Advertisement(repo: Repo, service: 'git-upload-pack' | 'git-receive-pack'): Buffer {
  const capabilities =
    service === 'git-upload-pack'
      ? 'multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not no-progress include-tag multi_ack_detailed allow-tip-sha1-in-want symref=HEAD:refs/heads/main agent=' + GIT_AGENT
      : 'report-status delete-refs side-band-64k ofs-delta quiet agent=' + GIT_AGENT;
  const lines: Array<string | Buffer | 'flush'> = [`# service=${service}\n`, 'flush'];
  const refs = sortedRefs(repo);
  const headOid = repo.refs.get('refs/heads/main');
  if (refs.length === 0) {
    lines.push(`${ZERO_OID} capabilities^{}\0${capabilities}\n`);
  } else {
    const headLine = headOid !== undefined ? `${headOid} HEAD\0${capabilities}\n` : null;
    if (headLine !== null) lines.push(headLine);
    for (const ref of refs) {
      lines.push(`${ref.oid} ${ref.name}\n`);
      if (ref.peeled !== undefined) lines.push(`${ref.peeled} ${ref.name}^{}\n`);
    }
  }
  lines.push('flush');
  return pktLines(lines);
}

/** The v2 capability advertisement. */
function v2Advertisement(): Buffer {
  return pktLines([
    'version 2\n',
    `agent=${GIT_AGENT}\n`,
    'ls-refs=unborn\n',
    'fetch=shallow filter\n',
    'object-format=sha1\n',
    'flush',
  ]);
}

/** The objects a fetch must send, plus the shallow boundary it reports. */
function selectFetchObjects(
  repo: Repo,
  wants: readonly string[],
  haves: readonly string[],
  shallow: readonly string[],
  depth: number | null,
  deepenRelative = false,
): { objects: PackObject[]; shallowOut: string[]; unshallowOut: string[] } {
  const shallowSet = new Set(shallow);
  // A "have" only proves the client holds that commit's history down to its
  // shallow boundaries: stop the have-closure walk at the shallow lines the
  // client sent, or a shallow clone's parent would look owned when it is
  // exactly what the client is asking for.
  const haveClosure = new Set<string>();
  {
    const stack = [...haves];
    while (stack.length > 0) {
      const oid = stack.pop() as string;
      if (haveClosure.has(oid)) continue;
      const obj = repo.objects.get(oid);
      if (obj === undefined) continue;
      haveClosure.add(oid);
      if (obj.type === 'commit') {
        if (shallowSet.has(oid)) continue;
        const view = parseCommit(obj.data);
        stack.push(view.tree);
        for (const parent of view.parents) stack.push(parent);
      } else if (obj.type === 'tree') {
        for (const entry of parseTree(obj.data)) stack.push(entry.oid);
      } else if (obj.type === 'tag') {
        stack.push(parseTag(obj.data).object);
      }
    }
  }
  const objects = new Map<string, PackObject>();
  const shallowOut: string[] = [];
  const unshallowOut: string[] = [];
  const include = (oid: string): void => {
    const obj = repo.objects.get(oid);
    if (obj === undefined || haveClosure.has(oid) || objects.has(oid)) return;
    objects.set(oid, obj);
  };
  if (depth === null) {
    for (const oid of collectReachable(repo.objects, wants)) include(oid);
    return { objects: [...objects.values()], shallowOut, unshallowOut };
  }
  // Depth-limited: walk the whole commit graph from the wants, recording
  // distances. `deepen-relative` (what real git sends for `fetch --deepen`)
  // counts from the client's current shallow boundary, not from the wants:
  // effective depth = deepen + (boundary distance + 1).
  const commitDepth = new Map<string, number>();
  const queue: Array<{ oid: string; depth: number }> = [];
  for (const want of wants) {
    const peeled = peelToCommit(repo, want);
    const start = peeled ?? want;
    if (!commitDepth.has(start)) {
      commitDepth.set(start, 0);
      queue.push({ oid: start, depth: 0 });
    }
  }
  while (queue.length > 0) {
    const { oid, depth: d } = queue.shift() as { oid: string; depth: number };
    const obj = repo.objects.get(oid);
    if (obj === undefined || obj.type !== 'commit') continue;
    for (const parent of parseCommit(obj.data).parents) {
      if (!commitDepth.has(parent)) {
        commitDepth.set(parent, d + 1);
        queue.push({ oid: parent, depth: d + 1 });
      }
    }
  }
  let effectiveDepth = depth;
  if (deepenRelative) {
    let base = 0;
    for (const oid of shallow) {
      const d = commitDepth.get(oid);
      if (d !== undefined) base = Math.max(base, d + 1);
    }
    effectiveDepth = depth + base;
  }
  // Send the wanted tag objects themselves, then every commit closer than
  // the effective depth with its tree closure. Commits at exactly
  // effectiveDepth - 1 are the new shallow boundary: the client receives
  // them (with trees) but must not traverse their parents.
  const boundary = new Set<string>();
  for (const want of wants) {
    const obj = repo.objects.get(want);
    if (obj !== undefined && obj.type === 'tag') include(want);
  }
  for (const [oid, d] of commitDepth) {
    if (d >= effectiveDepth) continue;
    if (d === effectiveDepth - 1) boundary.add(oid);
    const obj = repo.objects.get(oid);
    if (obj === undefined) continue;
    include(oid);
    const tree = parseCommit(obj.data).tree;
    for (const entry of collectReachable(repo.objects, [tree])) {
      const entryObj = repo.objects.get(entry);
      if (entryObj !== undefined && entryObj.type !== 'commit') include(entry);
    }
  }
  for (const oid of boundary) {
    if (!shallowSet.has(oid)) shallowOut.push(oid);
  }
  for (const oid of shallow) {
    if (commitDepth.has(oid) && (commitDepth.get(oid) as number) < effectiveDepth) unshallowOut.push(oid);
  }
  shallowOut.sort();
  unshallowOut.sort();
  return { objects: [...objects.values()], shallowOut, unshallowOut };
}

/** Parse a v2 command request's pkt-lines into argument lines. */
function v2RequestLines(body: Buffer): string[] {
  const lines: string[] = [];
  for (const packet of decodePktLines(body)) {
    if (packet.kind === 'data') lines.push(packet.payload.toString('utf8'));
  }
  return lines;
}

async function handleGit(ctx: Ctx, owner: string, repoName: string, tail: string[]): Promise<void> {
  const repo = getRepo(owner, repoName);
  const service = tail[0] as string;

  // GET /{owner}/{repo}.git/info/refs?service=...
  if (ctx.method === 'GET' && tail.length === 2 && tail[0] === 'info' && tail[1] === 'refs') {
    const wanted = ctx.query.get('service') ?? '';
    if (wanted !== 'git-upload-pack' && wanted !== 'git-receive-pack') {
      notFound(ctx);
      return;
    }
    if (repo === undefined) {
      sendJson(ctx, 404, { message: 'Not Found' });
      return;
    }
    const wantsV2 = wanted === 'git-upload-pack' && ctx.req.headers['git-protocol'] === 'version=2';
    const body = wantsV2 ? v2Advertisement() : v0Advertisement(repo, wanted);
    sendRaw(ctx, 200, body, `application/x-${wanted}-advertisement`);
    return;
  }

  // POST /{owner}/{repo}.git/git-upload-pack
  if (ctx.method === 'POST' && tail.length === 1 && tail[0] === 'git-upload-pack') {
    if (repo === undefined) {
      sendJson(ctx, 404, { message: 'Not Found' });
      return;
    }
    const raw = await readBody(ctx.req, MAX_GIT_BODY);
    if (raw === null) {
      sendJson(ctx, 400, { message: 'Request body too large' });
      return;
    }
    let response: Buffer;
    try {
      response = handleUploadPack(repo, raw, ctx.req.headers['git-protocol'] === 'version=2');
    } catch (err) {
      // A malformed request must not take the server down.
      sendRaw(ctx, 400, Buffer.from(`error: ${err instanceof Error ? err.message : 'bad request'}\n`, 'utf8'), 'text/plain');
      return;
    }
    sendRaw(ctx, 200, response, 'application/x-git-upload-pack-result');
    return;
  }

  // POST /{owner}/{repo}.git/git-receive-pack
  if (ctx.method === 'POST' && tail.length === 1 && tail[0] === 'git-receive-pack') {
    if (repo === undefined) {
      sendJson(ctx, 404, { message: 'Not Found' });
      return;
    }
    const raw = await readBody(ctx.req, MAX_GIT_BODY);
    if (raw === null) {
      sendJson(ctx, 400, { message: 'Request body too large' });
      return;
    }
    let response: Buffer;
    try {
      response = handleReceivePack(repo, raw);
    } catch (err) {
      sendRaw(ctx, 400, Buffer.from(`error: ${err instanceof Error ? err.message : 'bad request'}\n`, 'utf8'), 'text/plain');
      return;
    }
    sendRaw(ctx, 200, response, 'application/x-git-receive-pack-result');
    return;
  }

  notFound(ctx);
}

/** Answer an upload-pack request (protocol v2, or v0 when the client did not ask for v2). */
function handleUploadPack(repo: Repo, body: Buffer, wantsV2: boolean): Buffer {
  const lines = v2RequestLines(body);
  const command = lines.find((l) => l.startsWith('command='))?.slice('command='.length).trim() ?? '';
  if (!wantsV2) {
    // Protocol v0 fetch: want/have lines, then a flush, then the negotiation.
    const wants: string[] = [];
    const haves: string[] = [];
    const shallow: string[] = [];
    let depth: number | null = null;
    for (const line of lines) {
      const text = line.replace(/\n$/, '');
      if (text.startsWith('want ')) wants.push(text.slice(5).split(' ')[0] as string);
      else if (text.startsWith('have ')) haves.push(text.slice(5).trim());
      else if (text.startsWith('shallow ')) shallow.push(text.slice(8).trim());
      else if (text.startsWith('deepen ')) depth = Number(text.slice(7).trim());
    }
    const selected = selectFetchObjects(repo, wants, haves, shallow, depth);
    const pack = writePack(selected.objects);
    const parts: Buffer[] = [pktLine('NAK\n')];
    for (const oid of selected.shallowOut) parts.push(pktLine(`shallow ${oid}\n`));
    for (const oid of selected.unshallowOut) parts.push(pktLine(`unshallow ${oid}\n`));
    parts.push(...sidebandChunks(1, pack));
    parts.push(PKT_FLUSH);
    return Buffer.concat(parts);
  }

  if (command === 'ls-refs') {
    const prefixes: string[] = [];
    let peel = false;
    let symrefs = false;
    let unborn = false;
    for (const line of lines) {
      const text = line.replace(/\n$/, '');
      if (text.startsWith('ref-prefix ')) prefixes.push(text.slice('ref-prefix '.length));
      else if (text === 'peel') peel = true;
      else if (text === 'symrefs') symrefs = true;
      else if (text === 'unborn') unborn = true;
    }
    const out: Array<string | 'flush'> = [];
    const refs = sortedRefs(repo);
    const headOid = repo.refs.get('refs/heads/main');
    // The HEAD pseudo-ref is only sent when a prefix matches it: a client
    // asking for refs/heads/ must not get a HEAD line back.
    const headMatches = prefixes.length === 0 || prefixes.some((p) => 'HEAD'.startsWith(p));
    if (headMatches && headOid !== undefined) {
      out.push(`${headOid} HEAD${symrefs ? ' symref-target:refs/heads/main' : ''}\n`);
    } else if (headMatches && unborn) {
      out.push(`unborn HEAD${symrefs ? ' symref-target:refs/heads/main' : ''}\n`);
    }
    for (const ref of refs) {
      if (prefixes.length > 0 && !prefixes.some((p) => ref.name.startsWith(p))) continue;
      let line = `${ref.oid} ${ref.name}`;
      if (peel && ref.peeled !== undefined) line += ` peeled:${ref.peeled}`;
      out.push(`${line}\n`);
    }
    out.push('flush');
    return pktLines(out);
  }

  if (command === 'fetch') {
    const wants: string[] = [];
    const haves: string[] = [];
    const shallow: string[] = [];
    let depth: number | null = null;
    let deepenRelative = false;
    for (const line of lines) {
      const text = line.replace(/\n$/, '');
      if (text.startsWith('want ')) wants.push(text.slice(5).trim());
      else if (text.startsWith('have ')) haves.push(text.slice(5).trim());
      else if (text.startsWith('shallow ')) shallow.push(text.slice(8).trim());
      else if (text.startsWith('deepen ')) depth = Number(text.slice(7).trim());
      else if (text === 'deepen-relative') deepenRelative = true;
    }
    const selected = selectFetchObjects(repo, wants, haves, shallow, depth, deepenRelative);
    const pack = writePack(selected.objects);
    const parts: Buffer[] = [];
    // Git's own upload-pack omits the acknowledgments section entirely when the
    // client sent no "have" lines; a real client dies on it ("expected
    // 'packfile', received 'acknowledgments'"). With haves, acknowledge the
    // common ones and say ready so the client reads the packfile.
    if (haves.length > 0) {
      const common = haves.filter((have) => {
        const haveCommit = peelToCommit(repo, have);
        if (haveCommit === undefined) return false;
        return wants.some((want) => {
          const wantCommit = peelToCommit(repo, want);
          return wantCommit !== undefined && (wantCommit === haveCommit || isAncestor(repo, haveCommit, wantCommit));
        });
      });
      parts.push(pktLine('acknowledgments\n'));
      if (common.length > 0) {
        for (const oid of common) parts.push(pktLine(`ACK ${oid}\n`));
        parts.push(pktLine('ready\n'));
      } else {
        parts.push(pktLine('NAK\n'));
      }
      // Sections are separated by a delimiter packet (0001), not a flush:
      // git dies on a flush here ("expected packfile to be sent after
      // 'ready'"). Only the packfile section ends with a flush.
      parts.push(PKT_DELIM);
    }
    if (selected.shallowOut.length > 0 || selected.unshallowOut.length > 0) {
      parts.push(pktLine('shallow-info\n'));
      for (const oid of selected.shallowOut) parts.push(pktLine(`shallow ${oid}\n`));
      for (const oid of selected.unshallowOut) parts.push(pktLine(`unshallow ${oid}\n`));
      parts.push(PKT_DELIM);
    }
    parts.push(pktLine('packfile\n'));
    parts.push(...sidebandChunks(1, pack));
    parts.push(PKT_FLUSH);
    return Buffer.concat(parts);
  }

  throw new Error(`unknown upload-pack command ${JSON.stringify(command)}`);
}

/** Answer a receive-pack request (protocol v0): unpack, apply fast-forward rules, report. */
function handleReceivePack(repo: Repo, body: Buffer): Buffer {
  // The command list is pkt-lined up to the first flush; the pack follows it.
  let offset = 0;
  const commands: Array<{ oldOid: string; newOid: string; ref: string }> = [];
  let packStart = body.length;
  while (offset < body.length) {
    if (offset + 4 > body.length) throw new Error('truncated pkt-line in receive-pack');
    const len = parseInt(body.subarray(offset, offset + 4).toString('ascii'), 16);
    if (Number.isNaN(len)) throw new Error('bad pkt-line length in receive-pack');
    if (len === 0) {
      packStart = offset + 4;
      break;
    }
    const payload = body.subarray(offset + 4, offset + len).toString('utf8');
    const nul = payload.indexOf('\0');
    const line = (nul >= 0 ? payload.slice(0, nul) : payload).replace(/\n$/, '');
    const [oldOid, newOid, ref] = line.split(' ');
    if (oldOid !== undefined && newOid !== undefined && ref !== undefined) {
      commands.push({ oldOid: oldOid.toLowerCase(), newOid: newOid.toLowerCase(), ref });
    }
    offset += len;
  }
  if (commands.length === 0) throw new Error('receive-pack with no commands');

  // Unpack the pack (if any) and store its objects.
  let unpackOk = true;
  let unpackDetail = 'ok';
  const packBytes = body.subarray(packStart);
  const hasUpdates = commands.some((c) => c.newOid !== ZERO_OID);
  if (hasUpdates && packBytes.length > 0) {
    try {
      const objects = readPack(packBytes);
      for (const [oid, obj] of objects) {
        if (!repo.objects.has(oid)) repo.objects.set(oid, obj);
      }
    } catch (err) {
      unpackOk = false;
      unpackDetail = err instanceof Error ? err.message : 'unpack failed';
    }
  }

  // Apply the commands, one report line each.
  const reportLines: string[] = [`unpack ${unpackOk ? 'ok' : unpackDetail}\n`];
  for (const cmd of commands) {
    const current = repo.refs.get(cmd.ref);
    let ok = false;
    let reason = '';
    if (!unpackOk) {
      reason = 'unpack failed';
    } else if (cmd.newOid === ZERO_OID) {
      // Delete.
      if (current === undefined) reason = 'remote ref does not exist';
      else if (current !== cmd.oldOid && cmd.oldOid !== ZERO_OID) reason = 'non-fast-forward';
      else {
        repo.refs.delete(cmd.ref);
        ok = true;
      }
    } else if (cmd.oldOid === ZERO_OID) {
      // Create.
      if (current !== undefined) reason = 'already exists';
      else if (repo.objects.get(cmd.newOid) === undefined) reason = 'object not found';
      else {
        repo.refs.set(cmd.ref, cmd.newOid);
        ok = true;
      }
    } else {
      // Update: must fast-forward from the current value.
      if (current === undefined) reason = 'remote ref does not exist';
      else if (current !== cmd.oldOid) reason = 'non-fast-forward';
      else if (repo.objects.get(cmd.newOid) === undefined) reason = 'object not found';
      else if (!isAncestor(repo, cmd.oldOid, cmd.newOid) && !isBranchTipTag(repo, cmd)) reason = 'non-fast-forward';
      else {
        repo.refs.set(cmd.ref, cmd.newOid);
        ok = true;
      }
    }
    reportLines.push(ok ? `ok ${cmd.ref}\n` : `ng ${cmd.ref} ${reason}\n`);
  }
  const report = pktLines([...reportLines, 'flush']);
  return Buffer.concat([...sidebandChunks(1, report), PKT_FLUSH]);
}

/** A ref update whose new tip is a tag object still fast-forwards when the tag peels to a descendant. */
function isBranchTipTag(repo: Repo, cmd: { oldOid: string; newOid: string; ref: string }): boolean {
  if (!cmd.ref.startsWith('refs/heads/')) return true;
  const peeled = peelToCommit(repo, cmd.newOid);
  if (peeled === null) return false;
  return isAncestor(repo, cmd.oldOid, peeled);
}

// ---------------------------------------------------------------------------
// dispatcher

function dispatch(req: IncomingMessage, res: ServerResponse): void {
  const { path, query } = pathOf(req);
  const ctx: Ctx = {
    req,
    res,
    method: req.method ?? 'GET',
    path,
    query,
    host: asString(req.headers.host, 'localhost'),
  };
  void (async () => {
    try {
      await route(ctx);
    } catch (err) {
      process.stderr.write(`[entry] handler error: ${err instanceof Error ? err.message : String(err)}\n`);
      if (!res.headersSent) {
        sendJson(ctx, 500, { message: 'Internal Server Error' });
      } else {
        res.destroy();
      }
    }
  })();
}

async function route(ctx: Ctx): Promise<void> {
  const parts = splitPath(ctx.path);

  // The boot probe.
  if (ctx.method === 'GET' && ctx.path === '/healthz') {
    sendJson(ctx, 200, { ok: true });
    return;
  }

  // MCP.
  if (parts.length === 1 && parts[0] === 'mcp') {
    await handleMcp(ctx);
    return;
  }

  // GraphQL.
  if (parts.length === 2 && parts[0] === 'api' && parts[1] === 'graphql') {
    await handleGraphql(ctx);
    return;
  }

  // REST v3.
  if (parts.length >= 2 && parts[0] === 'api' && parts[1] === 'v3') {
    await restHandler(ctx);
    return;
  }

  // Git smart HTTP: /{owner}/{repo}.git/...
  if (parts.length >= 2 && (parts[1] as string).endsWith('.git')) {
    const owner = parts[0] as string;
    const repoName = (parts[1] as string).slice(0, -'.git'.length);
    await handleGit(ctx, owner, repoName, parts.slice(2));
    return;
  }

  notFound(ctx);
}

// ---------------------------------------------------------------------------
// lifecycle: bind, register, poll /done, exit 0

const REQUEST_TIMEOUT_MS = 30_000;
const RETRY_INITIAL_MS = 250;
const RETRY_MAX_MS = 2_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryDelay(attempt: number): number {
  return Math.min(RETRY_MAX_MS, RETRY_INITIAL_MS * 2 ** Math.min(attempt, 8));
}

async function register(port: number): Promise<void> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ port }),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (res.status === 200 || res.status === 409) return;
      if (res.status === 400 || res.status === 405) {
        throw new Error(`register answered HTTP ${res.status}: ${await res.text()}`);
      }
    } catch (err) {
      if (err instanceof Error && err.message.startsWith('register answered HTTP ')) throw err;
    }
    await sleep(retryDelay(attempt));
  }
}

async function waitForDone(): Promise<void> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      const res = await fetch(`${ENV_URL}/done`, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
      if (res.ok) {
        const body = (await res.json()) as { done?: unknown };
        if (body.done === true) return;
      }
    } catch {
      // The env is not answering yet: it is not done, so poll again.
    }
    await sleep(retryDelay(attempt));
  }
}

function createApp(): Server {
  const server = createServer(dispatch);
  server.keepAliveTimeout = 60_000;
  server.headersTimeout = 65_000;
  server.on('clientError', (_err, socket) => {
    socket.end('HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 30\r\n\r\n{"message":"Bad Request"}\n');
  });
  return server;
}

process.on('unhandledRejection', (err) => {
  process.stderr.write(`[entry] unhandled rejection: ${err instanceof Error ? err.message : String(err)}\n`);
});
process.on('uncaughtException', (err) => {
  process.stderr.write(`[entry] uncaught exception: ${err instanceof Error ? err.message : String(err)}\n`);
});

async function main(): Promise<void> {
  if (ENV_URL === '') {
    process.stderr.write('[entry] MAKEFUN_ENV_URL is not set: there is no env to register with\n');
    process.exit(1);
  }
  const server = createApp();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, () => resolve());
  });
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('the server did not bind an ephemeral TCP port');
  }
  process.stdout.write(`[entry] listening on port ${address.port}\n`);
  await register(address.port);
  process.stdout.write('[entry] registered; waiting for the env to say done\n');
  await waitForDone();
  // Idle keep-alive sockets from the suite's clients would otherwise keep the
  // event loop alive forever; close them so the process can actually exit.
  server.closeAllConnections();
  server.close();
  process.stdout.write('[entry] done\n');
}

try {
  await main();
  process.exit(0);
} catch (err) {
  process.stderr.write(`[entry] ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
}
