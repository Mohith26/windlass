'use strict';

// The replicated state machine. Deliberately boring: given the same sequence of
// commands it must produce the same sequence of results on every node, because
// the whole safety argument downstream depends on that.
//
// It also carries the client session table from section 6.3 of the Raft paper.
// Without it a client that retries after a timeout can get its command into the
// log twice, and then a single logical operation would take effect twice, which
// no amount of consensus correctness can fix.

class KvStore {
  constructor() {
    this.map = new Map();
    this.sessions = new Map(); // client id -> { seq, result }
    this.applied = 0;
    this.deduped = 0;
  }

  apply(cmd) {
    this.applied++;
    if (cmd === null || cmd === undefined) return null; // no-op entry

    if (cmd.cid !== undefined) {
      const last = this.sessions.get(cmd.cid);
      if (last !== undefined && cmd.seq <= last.seq) {
        this.deduped++;
        // Exactly the sequence number we already answered: hand back the same
        // answer. Anything older is a straggler from a request the client has
        // already given up on, and must not be replayed.
        return cmd.seq === last.seq ? last.result : { ok: true, stale: true, value: null };
      }
    }

    const result = this.execute(cmd);
    if (cmd.cid !== undefined) this.sessions.set(cmd.cid, { seq: cmd.seq, result: result });
    return result;
  }

  execute(cmd) {
    switch (cmd.op) {
      case 'put': {
        this.map.set(cmd.key, cmd.value);
        return { ok: true, value: cmd.value };
      }
      case 'get': {
        const has = this.map.has(cmd.key);
        return { ok: true, value: has ? this.map.get(cmd.key) : null };
      }
      case 'del': {
        const had = this.map.has(cmd.key);
        this.map.delete(cmd.key);
        return { ok: true, value: had };
      }
      case 'cas': {
        const cur = this.map.has(cmd.key) ? this.map.get(cmd.key) : null;
        if (cur === cmd.expect) {
          this.map.set(cmd.key, cmd.value);
          return { ok: true, value: true };
        }
        return { ok: true, value: false };
      }
      default:
        throw new Error('unknown command ' + JSON.stringify(cmd));
    }
  }

  // Order independent digest so two nodes can be compared cheaply. FNV-1a over
  // the sorted key/value pairs plus the session watermarks, since the session
  // table is part of the replicated state too. Collisions do not worry me here
  // because every test that compares digests also compares the full maps when
  // they disagree.
  digest() {
    let h = 0x811c9dc5;
    const mix = function (s) {
      for (let i = 0; i < s.length; i++) {
        h ^= s.charCodeAt(i);
        h = Math.imul(h, 0x01000193) >>> 0;
      }
    };
    for (const k of Array.from(this.map.keys()).sort()) mix(k + '=' + String(this.map.get(k)) + ';');
    mix('|');
    for (const c of Array.from(this.sessions.keys()).sort()) mix(c + '@' + this.sessions.get(c).seq + ';');
    return h >>> 0;
  }

  toObject() {
    const o = {};
    for (const k of Array.from(this.map.keys()).sort()) o[k] = this.map.get(k);
    return o;
  }
}

module.exports = { KvStore };
