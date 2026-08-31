'use strict';

const { Cluster } = require('./sim');
const { ClientPool } = require('./client');
const { InvariantChecker } = require('./invariants');
const { Rng } = require('./rand');
const linearizability = require('./linearizability');

// One seeded run: build a cluster, throw a scripted sequence of faults at it
// while clients keep working, then heal everything and check what survived.
//
// The fault script is generated up front from the seed so that a failing run can
// be printed and replayed exactly.

const SCENARIO_DEFAULTS = {
  seed: 1,
  n: 5,
  duration: 20000,
  settle: 6000,
  faultEvery: 900,
  maxDownFraction: 0.5, // fraction of the cluster allowed to be down at once
  dropRate: 0.02,
  dupRate: 0.01,
  latencyMin: 5,
  latencyMax: 40,
  clients: 3,
  keys: ['a', 'b', 'c'],
  timeout: 2500,
  checkLinearizability: true,
  raft: {},
  mutate: null,
};

// Purely random partitions turned out to be a weak fuzzer: the interesting Raft
// bugs all live in the moment a leader loses contact with a majority while some
// of its entries are already out there. So most of the weight goes on faults
// that are aimed at whoever is currently leading.
function buildFaultScript(rng, opts) {
  const script = [];
  const maxDown = Math.max(1, Math.floor(opts.n * opts.maxDownFraction));
  let t = opts.faultEvery;
  let partitioned = false;
  let down = 0;
  while (t < opts.duration) {
    const roll = rng.float();
    if (partitioned && roll < 0.30) {
      script.push({ at: t, kind: 'heal' });
      partitioned = false;
    } else if (down > 0 && roll < 0.50) {
      script.push({ at: t, kind: 'restart' });
      down--;
    } else if (roll < 0.62) {
      script.push({ at: t, kind: 'isolate-leader' });
      partitioned = true;
    } else if (roll < 0.74) {
      script.push({ at: t, kind: 'minority-leader' });
      partitioned = true;
    } else if (roll < 0.84) {
      script.push({ at: t, kind: 'partition' });
      partitioned = true;
    } else if (down < maxDown) {
      script.push({ at: t, kind: roll < 0.94 ? 'crash-leader' : 'crash' });
      down++;
    } else if (roll < 0.97) {
      script.push({ at: t, kind: 'quiesce' });
    } else {
      script.push({ at: t, kind: 'restart' });
      if (down > 0) down--;
    }
    if (script[script.length - 1].kind === 'quiesce') {
      script.push({ at: t + rng.range(300, 1200), kind: 'resume' });
    }
    t += rng.range(Math.floor(opts.faultEvery / 2), opts.faultEvery * 2);
  }
  script.sort(function (a, b) { return a.at - b.at; });
  return script;
}

function applyFault(cluster, fault, rng, pool) {
  switch (fault.kind) {
    case 'quiesce':
      if (pool) pool.paused = true;
      break;
    case 'resume':
      if (pool) pool.paused = false;
      break;
    case 'partition': {
      const ids = cluster.ids.slice();
      rng.shuffle(ids);
      const cut = rng.range(1, ids.length - 1);
      cluster.partition([ids.slice(0, cut), ids.slice(cut)]);
      fault.detail = ids.slice(0, cut).join('+') + ' | ' + ids.slice(cut).join('+');
      break;
    }
    case 'heal':
      cluster.heal();
      break;
    // Cut the leader off completely. Whatever it had replicated to a minority
    // just before the cut is exactly the material a Figure 8 style bug needs.
    case 'isolate-leader': {
      const ls = cluster.leaders();
      if (ls.length === 0) break;
      const lead = ls[0];
      cluster.isolate(lead);
      fault.detail = lead;
      break;
    }
    // Leave the leader connected to a minority so it keeps appending entries it
    // can never commit, then let the majority elect someone else.
    case 'minority-leader': {
      const ls = cluster.leaders();
      if (ls.length === 0) break;
      const lead = ls[0];
      const others = cluster.ids.filter(function (x) { return x !== lead; });
      rng.shuffle(others);
      const withLeader = others.slice(0, Math.max(0, Math.floor((cluster.ids.length - 1) / 2) - 1));
      const rest = others.slice(withLeader.length);
      cluster.partition([[lead].concat(withLeader), rest]);
      fault.detail = lead + '+' + withLeader.join('+') + ' | ' + rest.join('+');
      break;
    }
    case 'crash-leader': {
      const ls = cluster.leaders();
      const victim = ls.length > 0 ? ls[0] : null;
      if (victim === null) {
        const up = cluster.ids.filter(function (id) { return !cluster.down.has(id); });
        if (up.length === 0) break;
        cluster.crash(rng.pick(up));
        break;
      }
      cluster.crash(victim);
      fault.detail = victim;
      break;
    }
    case 'crash': {
      const up = cluster.ids.filter(function (id) { return !cluster.down.has(id); });
      if (up.length === 0) break;
      const victim = rng.pick(up);
      cluster.crash(victim);
      fault.detail = victim;
      break;
    }
    case 'restart': {
      const dead = Array.from(cluster.down);
      if (dead.length === 0) break;
      const lucky = rng.pick(dead);
      cluster.restart(lucky);
      fault.detail = lucky;
      break;
    }
    default:
      break;
  }
}

function runScenario(userOpts) {
  const opts = Object.assign({}, SCENARIO_DEFAULTS, userOpts || {});
  const cluster = new Cluster({
    n: opts.n, seed: opts.seed, dropRate: opts.dropRate, dupRate: opts.dupRate,
    latencyMin: opts.latencyMin, latencyMax: opts.latencyMax, raft: opts.raft,
    mutate: opts.mutate,
  });
  const checker = new InvariantChecker().attach(cluster);
  const pool = new ClientPool(cluster, {
    clients: opts.clients, keys: opts.keys, timeout: opts.timeout, seed: opts.seed ^ 0x5bf03635,
  });

  const faultRng = new Rng(opts.seed ^ 0x1234abcd);
  const script = buildFaultScript(faultRng, opts);
  let next = 0;

  const started = Date.now();
  while (cluster.now < opts.duration) {
    while (next < script.length && script[next].at <= cluster.now) {
      applyFault(cluster, script[next], faultRng, pool);
      next++;
    }
    if (!cluster.step()) cluster.runFor(10);
    pool.pump();
  }

  // Recovery window: no more faults, everything comes back, and the cluster gets
  // a fair chance to converge before anything is asserted about it.
  cluster.heal();
  pool.paused = false;
  for (const id of Array.from(cluster.down)) cluster.restart(id);
  const settleUntil = cluster.now + opts.settle;
  while (cluster.now < settleUntil) {
    if (!cluster.step()) cluster.runFor(10);
    pool.pump();
  }
  pool.finish();

  const violations = checker.finalCheck(cluster);
  const convergence = checkConvergence(cluster);

  let lin = null;
  if (opts.checkLinearizability) {
    lin = linearizability.check(pool.history, { limit: 300000 });
  }

  return {
    opts: opts,
    cluster: cluster,
    pool: pool,
    checker: checker,
    script: script,
    violations: violations,
    convergence: convergence,
    linearizability: lin,
    wallMs: Date.now() - started,
    ok: violations.length === 0 && convergence.ok && (lin === null || lin.ok),
  };
}

// After the settle window every node should hold the same log prefix and, at
// equal applied indices, the same state machine contents.
function checkConvergence(cluster) {
  const live = cluster.ids.filter(function (id) { return !cluster.down.has(id); });
  const problems = [];
  let minCommit = Infinity;
  for (const id of live) minCommit = Math.min(minCommit, cluster.nodes.get(id).commitIndex);

  const ref = cluster.nodes.get(live[0]);
  for (const id of live.slice(1)) {
    const node = cluster.nodes.get(id);
    for (let i = 1; i <= minCommit; i++) {
      const a = ref.log[i], b = node.log[i];
      if (!a || !b || a.term !== b.term || JSON.stringify(a.cmd) !== JSON.stringify(b.cmd)) {
        problems.push('log divergence at index ' + i + ' between ' + ref.id + ' and ' + id);
        break;
      }
    }
  }

  const byApplied = new Map();
  for (const id of live) {
    const node = cluster.nodes.get(id);
    const key = node.lastApplied;
    if (!byApplied.has(key)) byApplied.set(key, []);
    byApplied.get(key).push(id);
  }
  for (const [applied, ids] of byApplied) {
    if (ids.length < 2) continue;
    const d = cluster.machines.get(ids[0]).digest();
    for (const id of ids.slice(1)) {
      if (cluster.machines.get(id).digest() !== d) {
        problems.push('state machines differ at applied index ' + applied + ': ' + ids.join(','));
      }
    }
  }

  return { ok: problems.length === 0, problems: problems, minCommit: minCommit, live: live.length };
}

module.exports = { runScenario, buildFaultScript, checkConvergence, SCENARIO_DEFAULTS };
