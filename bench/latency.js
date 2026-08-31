'use strict';
const { Cluster } = require('../src/sim');
const { ClientPool } = require('../src/client');
const { save, summarise } = require('./util');

// Two timings that matter and that the simulator can measure exactly, because
// virtual time has no jitter in it: how long a cluster takes to elect a new
// leader after the old one dies, and what a client sees end to end.

function electionAfterLeaderCrash(seed, n) {
  const c = new Cluster({ n: n, seed: seed, latencyMin: 5, latencyMax: 20 });
  c.runFor(2000);
  const old = c.liveLeader();
  if (old === null) return null;
  c.crash(old.id);
  const start = c.now;
  const deadline = c.now + 8000;
  while (c.now < deadline) {
    c.step();
    const fresh = c.liveLeader();
    if (fresh !== null && fresh.id !== old.id) return c.now - start;
  }
  return null;
}

function clientLatency(seed, opts) {
  const c = new Cluster(Object.assign({ n: 5, seed: seed }, opts.cluster || {}));
  const pool = new ClientPool(c, { clients: opts.clients || 3, keys: ['a', 'b', 'c', 'd', 'e'], seed: seed ^ 991 });
  while (c.now < (opts.duration || 20000)) {
    if (!c.step()) c.runFor(10);
    if (opts.faults) opts.faults(c);
    pool.pump();
  }
  pool.finish();
  const open = new Map();
  const lat = [];
  for (const ev of pool.history) {
    if (ev.type === 'invoke') open.set(ev.id, ev.time);
    else if (ev.type === 'ok' && open.has(ev.id)) { lat.push(ev.time - open.get(ev.id)); open.delete(ev.id); }
  }
  return { latencies: lat, stats: pool.stats, committed: c.nodes.get('n0').commitIndex };
}

function main(seeds) {
  const total = seeds || 60;
  const started = Date.now();

  const elections = { 3: [], 5: [], 7: [] };
  let missed = 0;
  for (const n of [3, 5, 7]) {
    for (let seed = 1; seed <= total; seed++) {
      const t = electionAfterLeaderCrash(seed, n);
      if (t === null) missed++; else elections[n].push(t);
    }
  }

  const quiet = [];
  const lossy = [];
  let quietOps = 0, lossyOps = 0;
  for (let seed = 1; seed <= 20; seed++) {
    const a = clientLatency(seed, { duration: 20000 });
    quiet.push.apply(quiet, a.latencies);
    quietOps += a.stats.ok;
    const b = clientLatency(seed, { duration: 20000, cluster: { dropRate: 0.15, dupRate: 0.05, latencyMax: 60 } });
    lossy.push.apply(lossy, b.latencies);
    lossyOps += b.stats.ok;
  }

  const out = {
    electionAfterLeaderCrashVirtualMs: {
      3: summarise(elections[3]), 5: summarise(elections[5]), 7: summarise(elections[7]),
      noLeaderWithinWindow: missed,
      note: 'election timeout window is 150 to 300 virtual ms, one way link latency 5 to 20',
    },
    clientLatencyVirtualMs: {
      quietNetwork: summarise(quiet),
      lossyNetwork: summarise(lossy),
      quietOps: quietOps, lossyOps: lossyOps,
    },
    wallMs: Date.now() - started,
  };
  const file = save('latency.json', out);
  for (const n of [3, 5, 7]) {
    const s = out.electionAfterLeaderCrashVirtualMs[n];
    console.log('  n=' + n + ' election after leader crash p50=' + s.p50 + ' p99=' + s.p99 + ' max=' + s.max + ' virtual ms');
  }
  console.log('  client latency quiet p50/p99=' + out.clientLatencyVirtualMs.quietNetwork.p50 + '/' +
    out.clientLatencyVirtualMs.quietNetwork.p99 + ', lossy p50/p99=' +
    out.clientLatencyVirtualMs.lossyNetwork.p50 + '/' + out.clientLatencyVirtualMs.lossyNetwork.p99);
  console.log('  wrote ' + file);
  return out;
}

module.exports = { main };

if (require.main === module) main();
