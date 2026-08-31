'use strict';

// The replicated state machine. Deliberately boring: given the same sequence of
// commands it must produce the same sequence of results on every node, because
// the whole safety argument downstream depends on that.

class KvStore {
  constructor() {
    this.map = new Map();
    this.applied = 0;
  }

  apply(cmd) {
    this.applied++;
    if (cmd === null || cmd === undefined) return null; // no-op entry
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
  // the sorted key/value pairs; collisions are irrelevant at this scale and the
  // tests compare the full maps anyway when a digest mismatch shows up.
  digest() {
    const keys = Array.from(this.map.keys()).sort();
    let h = 0x811c9dc5;
    for (const k of keys) {
      const s = k + '=' + String(this.map.get(k)) + ';';
      for (let i = 0; i < s.length; i++) {
        h ^= s.charCodeAt(i);
        h = Math.imul(h, 0x01000193) >>> 0;
      }
    }
    return h >>> 0;
  }

  toObject() {
    const o = {};
    for (const k of Array.from(this.map.keys()).sort()) o[k] = this.map.get(k);
    return o;
  }
}

module.exports = { KvStore };
