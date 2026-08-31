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
  dropRate: 0.02,
  dupRate: 0.01,
  latencyMin: 5,
  latencyMax: 40,
  clients: 3,
  keys: ['a', 'b', 'c'],
  timeout: 2500,
  checkLinearizability: true,
  raft: {},
};

function buildFaultScript(rng, opts) {
  const script = [];
  const maxDown = Math.floor((opts.n - 1) / 2);
  let t = opts.faultEvery;
  let partitioned = false;
  let down = 0;
  while (t < opts.duration) {
    const roll = rng.float();
    if (partitioned && roll < 0.35) {
      script.push({ at: t, kind: 'heal' });
      partitioned = false;
    } else if (down > 0 && roll < 0.55) {
      script.push({ at: t, kind: 'restart' });
      down--;
    } else if (!partitioned && roll < 0.78) {
      script.push({ at: t, kind: 'partition' });
      partitioned = true;
    } else if (down < maxDown) {
      script.push({ at: t, kind: 'crash' });
      down++;
    } else {
      script.push({ at: t, kind: 'noop' });
    }
    t += rng.range(Math.floor(opts.faultEvery / 2), opts.faultEvery * 2);
  }
  return script;
}

function applyFault(cluster, fault, rng) {
  switch (fault.kind) {
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
    default:
      break;
  }
}

function runScenario(userOpts) {
  const opts = Object.assign({}, SCENARIO_DEFAULTS, userOpts || {});
  const cluster = new Cluster({
    n: opts.n, seed: opts.seed, dropRate: opts.dropRate, dupRate: opts.dupRate,
    latencyMin: opts.latencyMin, latencyMax: opts.latencyMax, raft: opts.raft,
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
      applyFault(cluster, script[next], faultRng);
      next++;
    }
    if (!cluster.step()) cluster.runFor(10);
    pool.pump();
  }

  // Recovery window: no more faults, everything comes back, and the cluster gets
  // a fair chance to converge before anything is asserted about it.
  cluster.heal();
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
