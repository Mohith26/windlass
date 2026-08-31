'use strict';

const { Rng } = require('./rand');
const { LEADER } = require('./raft');

// Simulated clients. Each one has a single operation outstanding at a time,
// keeps a sequence number so the state machine can suppress its retries, and
// records a history in the shape the linearizability checker expects.
//
// The three outcomes are the ones a real client sees: ok, an explicit failure
// (nobody would take the request), and a timeout where the client genuinely does
// not know whether the operation happened.

const CLIENT_DEFAULTS = {
  clients: 3,
  keys: ['a', 'b', 'c'],
  timeout: 2000,
  thinkMin: 5,
  thinkMax: 40,
  seed: 7,
  values: 100,
};

class ClientPool {
  constructor(cluster, opts) {
    const o = Object.assign({}, CLIENT_DEFAULTS, opts || {});
    this.opts = o;
    this.c = cluster;
    this.rng = new Rng(o.seed);
    this.history = [];
    this.stats = { issued: 0, ok: 0, timedOut: 0, retries: 0, redirects: 0 };
    this.clients = [];
    for (let i = 0; i < o.clients; i++) {
      this.clients.push({
        cid: 'c' + i, seq: 0, pending: null, nextIssueAt: 0, hint: null,
      });
    }
    this.cursor = 0;
    const self = this;
    cluster.observe(function () { self.harvest(); });
  }

  randomOp() {
    const r = this.rng;
    const key = r.pick(this.opts.keys);
    const roll = r.float();
    if (roll < 0.35) return { op: 'put', key: key, value: r.int(this.opts.values) };
    if (roll < 0.70) return { op: 'get', key: key };
    if (roll < 0.85) return { op: 'del', key: key };
    return { op: 'cas', key: key, expect: r.int(this.opts.values), value: r.int(this.opts.values) };
  }

  targetFor(client) {
    const c = this.c;
    const up = c.ids.filter(function (id) { return !c.down.has(id); });
    if (up.length === 0) return null;
    if (client.hint !== null && up.indexOf(client.hint) >= 0) return client.hint;
    return this.rng.pick(up);
  }

  // Called by the driver after each simulator step.
  pump() {
    const now = this.c.now;
    for (const client of this.clients) {
      if (client.pending !== null) {
        if (now - client.pending.startedAt >= this.opts.timeout) {
          this.history.push({ type: 'info', id: client.pending.id, proc: client.cid, op: client.pending.op, time: now });
          this.stats.timedOut++;
          client.pending = null;
          client.hint = null;
          client.nextIssueAt = now + this.rng.range(this.opts.thinkMin, this.opts.thinkMax);
        } else {
          this.attempt(client, now);
        }
        continue;
      }
      if (now >= client.nextIssueAt) {
        client.seq++;
        const op = this.randomOp();
        const id = client.cid + '#' + client.seq;
        client.pending = { id: id, op: op, startedAt: now, lastTryAt: -1e9, node: null };
        this.history.push({ type: 'invoke', id: id, proc: client.cid, op: op, time: now });
        this.stats.issued++;
        this.attempt(client, now);
      }
    }
  }

  // Try to hand the pending request to a node. Retries are throttled so a
  // client does not spam a node on every single simulator step.
  attempt(client, now) {
    const p = client.pending;
    if (now - p.lastTryAt < 50) return;
    p.lastTryAt = now;
    const targetId = this.targetFor(client);
    if (targetId === null) return;
    const node = this.c.nodes.get(targetId);
    if (node.state !== LEADER) {
      client.hint = node.leaderId;
      this.stats.redirects++;
      return;
    }
    const cmd = Object.assign({ cid: client.cid, seq: client.seq }, p.op);
    const before = node.state;
    const res = node.propose(cmd, now);
    this.c.afterNodeStep(node, before);
    if (res.ok) {
      p.node = targetId;
      client.hint = targetId;
      if (p.attempts === undefined) p.attempts = 0;
      p.attempts++;
      if (p.attempts > 1) this.stats.retries++;
    } else {
      client.hint = res.leaderHint;
      this.stats.redirects++;
    }
  }

  // Watches the cluster apply log for the entries this pool is waiting on. A
  // client only learns a result from the node it is talking to, which is why
  // the node id is part of the match.
  harvest() {
    const log = this.c.applyLog;
    for (; this.cursor < log.length; this.cursor++) {
      const a = log[this.cursor];
      if (!a.cmd || a.cmd.cid === undefined) continue;
      for (const client of this.clients) {
        const p = client.pending;
        if (p === null) continue;
        if (a.cmd.cid !== client.cid || a.cmd.seq !== client.seq) continue;
        if (a.id !== p.node) continue;
        this.history.push({
          type: 'ok', id: p.id, proc: client.cid, op: p.op,
          result: a.result === null ? null : a.result.value, time: this.c.now,
        });
        this.stats.ok++;
        client.pending = null;
        client.nextIssueAt = this.c.now + this.rng.range(this.opts.thinkMin, this.opts.thinkMax);
        break;
      }
    }
  }

  // Anything still outstanding at the end of a run is reported as unknown
  // rather than quietly dropped, because the checker has to account for it.
  finish() {
    for (const client of this.clients) {
      if (client.pending !== null) {
        this.history.push({ type: 'info', id: client.pending.id, proc: client.cid, op: client.pending.op, time: this.c.now });
        this.stats.timedOut++;
        client.pending = null;
      }
    }
    return this.history;
  }
}

module.exports = { ClientPool, CLIENT_DEFAULTS };
