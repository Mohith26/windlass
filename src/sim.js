'use strict';

const { Rng } = require('./rand');
const { RaftNode, LEADER } = require('./raft');
const { KvStore } = require('./kv');

// A discrete event simulator for the whole cluster. Virtual time only, one
// event queue, one PRNG. Two runs with the same seed and the same script have
// to produce identical event traces, otherwise a failing seed is useless to me.

class MinHeap {
  constructor() { this.a = []; }
  get size() { return this.a.length; }
  // Order by delivery time, then by insertion sequence so ties are stable.
  less(i, j) {
    const x = this.a[i], y = this.a[j];
    return x.at !== y.at ? x.at < y.at : x.seq < y.seq;
  }
  push(item) {
    const a = this.a;
    a.push(item);
    let i = a.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.less(i, p)) break;
      const t = a[i]; a[i] = a[p]; a[p] = t;
      i = p;
    }
  }
  peek() { return this.a[0]; }
  pop() {
    const a = this.a;
    const top = a[0];
    const last = a.pop();
    if (a.length > 0) {
      a[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1, r = l + 1;
        let m = i;
        if (l < a.length && this.less(l, m)) m = l;
        if (r < a.length && this.less(r, m)) m = r;
        if (m === i) break;
        const t = a[i]; a[i] = a[m]; a[m] = t;
        i = m;
      }
    }
    return top;
  }
}

const SIM_DEFAULTS = {
  n: 5,
  seed: 1,
  latencyMin: 5,
  latencyMax: 30,
  dropRate: 0,
  dupRate: 0,
  raft: {},
  // Optional hook used by the mutation tests to hand back a deliberately broken
  // node. It runs on every node, including the ones rebuilt by a restart.
  mutate: null,
};

class Cluster {
  constructor(opts) {
    const o = Object.assign({}, SIM_DEFAULTS, opts || {});
    this.opts = o;
    this.rng = new Rng(o.seed);
    this.now = 0;
    this.seq = 0;
    this.queue = new MinHeap();

    this.ids = [];
    for (let i = 0; i < o.n; i++) this.ids.push('n' + i);

    this.nodes = new Map();
    this.machines = new Map();
    this.disk = new Map();
    this.down = new Set();
    // null means fully connected. Otherwise a list of groups; two nodes can
    // talk only if they share a group.
    this.groups = null;

    for (const id of this.ids) {
      const node = new RaftNode(id, this.ids, Object.assign({ rng: this.rng }, o.raft));
      if (o.mutate) o.mutate(node);
      this.nodes.set(id, node);
      this.machines.set(id, new KvStore());
      this.disk.set(id, node.persistentState());
      this.persistMark = this.persistMark || new Map();
      this.persistMark.set(id, node.persistOps);
    }

    this.stats = {
      messagesSent: 0, messagesDelivered: 0, messagesDropped: 0,
      messagesDuplicated: 0, partitionDropped: 0, crashes: 0,
      elections: 0, persistOps: 0, steps: 0,
    };

    this.observers = [];
    this.applyLog = []; // { id, index, term, cmd } in application order
  }

  // ---- topology ------------------------------------------------------------

  reachable(a, b) {
    if (this.down.has(a) || this.down.has(b)) return false;
    if (this.groups === null) return true;
    for (const g of this.groups) {
      if (g.indexOf(a) >= 0 && g.indexOf(b) >= 0) return true;
    }
    return false;
  }

  partition(groups) { this.groups = groups.map(function (g) { return g.slice(); }); }
  heal() { this.groups = null; }
  isolate(id) {
    const rest = this.ids.filter(function (x) { return x !== id; });
    this.partition([[id], rest]);
  }

  crash(id) {
    if (this.down.has(id)) return;
    this.down.add(id);
    this.stats.crashes++;
  }

  // Restart wipes every piece of volatile state: the node comes back with only
  // what was on disk, and the state machine has to be rebuilt by replaying the
  // committed log, exactly like a real restart with no snapshot support.
  restart(id) {
    if (!this.down.has(id)) return;
    this.down.delete(id);
    const node = new RaftNode(id, this.ids, Object.assign({ rng: this.rng }, this.opts.raft));
    if (this.opts.mutate) this.opts.mutate(node);
    node.restore(this.disk.get(id));
    node.resetElectionTimer(this.now);
    this.nodes.set(id, node);
    this.machines.set(id, new KvStore());
    this.persistMark.set(id, node.persistOps);
  }

  // ---- event plumbing ------------------------------------------------------

  schedule(at, fn) {
    this.queue.push({ at: at, seq: this.seq++, fn: fn });
  }

  dispatch(msgs) {
    for (const msg of msgs) {
      this.stats.messagesSent++;
      if (!this.reachable(msg.from, msg.to)) { this.stats.partitionDropped++; continue; }
      if (this.opts.dropRate > 0 && this.rng.chance(this.opts.dropRate)) {
        this.stats.messagesDropped++;
        continue;
      }
      const copies = (this.opts.dupRate > 0 && this.rng.chance(this.opts.dupRate)) ? 2 : 1;
      if (copies === 2) this.stats.messagesDuplicated++;
      for (let c = 0; c < copies; c++) {
        const delay = this.rng.range(this.opts.latencyMin, this.opts.latencyMax);
        const self = this;
        this.schedule(this.now + delay, function () { self.deliver(msg); });
      }
    }
  }

  deliver(msg) {
    // Re-check reachability at delivery time: a message already in flight when
    // a partition appears should not sneak across it.
    if (!this.reachable(msg.from, msg.to)) { this.stats.partitionDropped++; return; }
    const node = this.nodes.get(msg.to);
    const before = node.state;
    node.handle(msg, this.now);
    this.stats.messagesDelivered++;
    this.afterNodeStep(node, before);
  }

  afterNodeStep(node, stateBefore) {
    // Persist first, then release anything the node wants to send. A response
    // that escapes before its own vote is on disk is the classic way to break
    // election safety across a crash.
    if (node.persistOps !== this.persistMark.get(node.id)) {
      this.stats.persistOps += node.persistOps - this.persistMark.get(node.id);
      this.persistMark.set(node.id, node.persistOps);
      this.disk.set(node.id, node.persistentState());
    }
    if (stateBefore !== LEADER && node.state === LEADER) this.stats.elections++;

    const applied = node.takeApplied();
    if (applied.length > 0) {
      const machine = this.machines.get(node.id);
      for (const e of applied) {
        const result = machine.apply(e.cmd);
        this.applyLog.push({ id: node.id, index: e.index, term: e.term, cmd: e.cmd, result: result });
      }
    }
    this.dispatch(node.drain());
    for (const obs of this.observers) obs(this);
  }

  nextTimerAt() {
    let best = Infinity;
    for (const id of this.ids) {
      if (this.down.has(id)) continue;
      const node = this.nodes.get(id);
      const t = node.state === LEADER ? node.heartbeatDeadline : node.electionDeadline;
      if (t < best) best = t;
    }
    return best;
  }

  // Advance to the next interesting instant and process everything scheduled
  // for it. Returns false when nothing is left to do.
  step() {
    const evAt = this.queue.size > 0 ? this.queue.peek().at : Infinity;
    const timerAt = this.nextTimerAt();
    const next = Math.min(evAt, timerAt);
    if (next === Infinity) return false;
    this.now = Math.max(this.now, next);
    this.stats.steps++;

    while (this.queue.size > 0 && this.queue.peek().at <= this.now) {
      const ev = this.queue.pop();
      ev.fn();
    }
    for (const id of this.ids) {
      if (this.down.has(id)) continue;
      const node = this.nodes.get(id);
      const before = node.state;
      node.tick(this.now);
      this.afterNodeStep(node, before);
    }
    return true;
  }

  runUntil(deadline) {
    while (this.now < deadline) {
      const evAt = this.queue.size > 0 ? this.queue.peek().at : Infinity;
      const next = Math.min(evAt, this.nextTimerAt());
      if (next === Infinity || next > deadline) { this.now = deadline; break; }
      this.step();
    }
  }

  runFor(ms) { this.runUntil(this.now + ms); }

  // ---- inspection ----------------------------------------------------------

  leaders() {
    const out = [];
    for (const id of this.ids) {
      if (this.down.has(id)) continue;
      if (this.nodes.get(id).state === LEADER) out.push(id);
    }
    return out;
  }

  leader() {
    const ls = this.leaders();
    return ls.length === 1 ? this.nodes.get(ls[0]) : null;
  }

  // A leader that can actually reach a quorum, which is the one a client can
  // make progress against.
  liveLeader() {
    for (const id of this.leaders()) {
      let reach = 1;
      for (const other of this.ids) {
        if (other !== id && this.reachable(id, other)) reach++;
      }
      if (reach >= this.nodes.get(id).quorum) return this.nodes.get(id);
    }
    return null;
  }

  observe(fn) { this.observers.push(fn); }
}

module.exports = { Cluster, MinHeap, SIM_DEFAULTS };
