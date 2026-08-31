'use strict';
const { test, ok, eq } = require('./harness');
const { runScenario } = require('../src/scenario');
const mutants = require('../src/mutants');

// The sweep the suite runs on every invocation is deliberately small. The long
// sweeps live in bench/ and write their numbers to results/.
const SEEDS = 20;
const BASE = {
  n: 5, duration: 9000, settle: 4000, dropRate: 0.05, dupRate: 0.02,
  latencyMax: 60, faultEvery: 350,
};

function sweep(extra, seeds) {
  const failures = [];
  let ops = 0, committed = 0;
  for (let seed = 1; seed <= (seeds || SEEDS); seed++) {
    const r = runScenario(Object.assign({ seed: seed }, BASE, extra || {}));
    ops += r.pool.stats.ok;
    committed += r.convergence.minCommit;
    if (!r.ok) failures.push({ seed: seed, violations: r.violations.slice(0, 3), convergence: r.convergence.problems });
  }
  return { failures: failures, ops: ops, committed: committed };
}

test('a five node cluster survives the standard fault sweep', function () {
  const r = sweep();
  eq(r.failures.length, 0, JSON.stringify(r.failures.slice(0, 2)));
  ok(r.ops > 2000, 'the sweep should actually be doing work, saw ' + r.ops);
});

test('a three node cluster survives the same sweep', function () {
  const r = sweep({ n: 3 }, 10);
  eq(r.failures.length, 0, JSON.stringify(r.failures.slice(0, 2)));
});

test('a seven node cluster survives the same sweep', function () {
  const r = sweep({ n: 7 }, 10);
  eq(r.failures.length, 0, JSON.stringify(r.failures.slice(0, 2)));
});

test('heavy message loss delays progress but never breaks safety', function () {
  const r = sweep({ dropRate: 0.3, dupRate: 0.1 }, 10);
  eq(r.failures.length, 0, JSON.stringify(r.failures.slice(0, 2)));
});

test('every client history in the sweep is linearizable', function () {
  let checked = 0;
  for (let seed = 1; seed <= 10; seed++) {
    const r = runScenario(Object.assign({ seed: seed }, BASE));
    ok(r.linearizability !== null);
    ok(!r.linearizability.unknown, 'seed ' + seed + ' exhausted the search budget');
    ok(r.linearizability.ok, 'seed ' + seed + ' is not linearizable');
    checked++;
  }
  eq(checked, 10);
});

test('a scenario replays identically from the same seed', function () {
  const a = runScenario(Object.assign({ seed: 5 }, BASE));
  const b = runScenario(Object.assign({ seed: 5 }, BASE));
  eq(JSON.stringify(a.cluster.stats), JSON.stringify(b.cluster.stats));
  eq(JSON.stringify(a.pool.stats), JSON.stringify(b.pool.stats));
  eq(a.pool.history.length, b.pool.history.length);
});

test('the fault script is part of the seed and can be printed for a repro', function () {
  const r = runScenario(Object.assign({ seed: 5 }, BASE));
  ok(r.script.length > 5);
  ok(r.script.every(function (f) { return typeof f.at === 'number' && typeof f.kind === 'string'; }));
});

test('clients genuinely see timeouts and retries under these faults', function () {
  let retries = 0, timeouts = 0;
  for (let seed = 1; seed <= 10; seed++) {
    const r = runScenario(Object.assign({ seed: seed }, BASE));
    retries += r.pool.stats.retries;
    timeouts += r.pool.stats.timedOut;
  }
  ok(retries > 0, 'no retries means the faults are too gentle to be interesting');
  ok(timeouts > 0, 'no timeouts means the same');
});

// Mutation coverage: each of these has to fail somewhere inside a short sweep.
const FUZZ_CATCHABLE = ['vote-without-log-check', 'small-quorum', 'blind-truncate', 'accept-stale-append'];
for (const name of FUZZ_CATCHABLE) {
  test('the fuzzer catches the ' + name + ' mutant', function () {
    let caught = 0;
    for (let seed = 1; seed <= 30 && caught === 0; seed++) {
      const r = runScenario(Object.assign({ seed: seed }, BASE, { mutate: mutants.get(name) }));
      if (!r.ok) caught++;
    }
    eq(caught, 1, name + ' survived thirty seeds');
  });
}

test('every mutant is covered by either the fuzzer or a scripted case', function () {
  const scripted = ['commit-across-terms', 'forget-vote-on-crash'];
  const covered = FUZZ_CATCHABLE.concat(scripted).sort();
  eq(JSON.stringify(covered), JSON.stringify(mutants.list().sort()));
});
