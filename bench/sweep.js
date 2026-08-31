'use strict';
const { runScenario } = require('../src/scenario');
const { save, summarise } = require('./util');

// The long randomised sweep. Every profile runs the same seeds so that a
// difference between profiles is a difference in the fault model and nothing
// else.
const PROFILES = [
  { name: 'clean-network', n: 5, dropRate: 0, dupRate: 0, faultEvery: 100000 },
  { name: 'lossy-network', n: 5, dropRate: 0.15, dupRate: 0.05, faultEvery: 100000 },
  { name: 'faults-5', n: 5, dropRate: 0.05, dupRate: 0.02, faultEvery: 350 },
  { name: 'faults-3', n: 3, dropRate: 0.05, dupRate: 0.02, faultEvery: 350 },
  { name: 'faults-7', n: 7, dropRate: 0.05, dupRate: 0.02, faultEvery: 350 },
  { name: 'brutal', n: 5, dropRate: 0.30, dupRate: 0.10, faultEvery: 200, latencyMax: 120 },
];

function main(seeds) {
  const total = seeds || 100;
  const started = Date.now();
  const out = { seedsPerProfile: total, profiles: [], totals: {} };
  let allOps = 0, allCommitted = 0, allViolations = 0, allSteps = 0, allMessages = 0, allRuns = 0;

  for (const profile of PROFILES) {
    const opts = Object.assign({ duration: 12000, settle: 5000, latencyMax: 60 }, profile);
    delete opts.name;
    const failures = [];
    const latencies = [];
    let ops = 0, timeouts = 0, committed = 0, messages = 0, steps = 0, elections = 0, persistOps = 0;
    let linUnknown = 0;
    const t0 = Date.now();
    for (let seed = 1; seed <= total; seed++) {
      const r = runScenario(Object.assign({ seed: seed }, opts));
      allRuns++;
      ops += r.pool.stats.ok;
      timeouts += r.pool.stats.timedOut;
      committed += r.convergence.minCommit;
      messages += r.cluster.stats.messagesSent;
      steps += r.cluster.stats.steps;
      elections += r.cluster.stats.elections;
      persistOps += r.cluster.stats.persistOps;
      if (r.linearizability && r.linearizability.unknown) linUnknown++;
      const open = new Map();
      for (const ev of r.pool.history) {
        if (ev.type === 'invoke') open.set(ev.id, ev.time);
        else if (ev.type === 'ok' && open.has(ev.id)) {
          latencies.push(ev.time - open.get(ev.id));
          open.delete(ev.id);
        }
      }
      if (!r.ok) {
        failures.push({
          seed: seed,
          violations: r.violations.slice(0, 3),
          convergence: r.convergence.problems.slice(0, 3),
          linearizable: r.linearizability ? r.linearizability.ok : null,
        });
      }
    }
    const wall = Date.now() - t0;
    allOps += ops; allCommitted += committed; allViolations += failures.length;
    allSteps += steps; allMessages += messages;
    out.profiles.push({
      name: profile.name, nodes: opts.n, seeds: total, wallMs: wall,
      clientOpsCompleted: ops, clientOpsTimedOut: timeouts,
      committedEntries: committed, messagesSent: messages, simulatorSteps: steps,
      elections: elections, persistOps: persistOps,
      linearizabilityUnknown: linUnknown,
      failures: failures.length, failureDetail: failures.slice(0, 5),
      clientLatencyVirtualMs: summarise(latencies),
      seedsPerSecond: Math.round((total / (wall / 1000)) * 100) / 100,
    });
  }

  const wallMs = Date.now() - started;
  out.totals = {
    runs: allRuns, wallMs: wallMs,
    clientOpsCompleted: allOps, committedEntries: allCommitted,
    simulatorSteps: allSteps, messagesSent: allMessages,
    failures: allViolations,
    simulatorStepsPerSecond: Math.round(allSteps / (wallMs / 1000)),
    committedEntriesPerSecond: Math.round(allCommitted / (wallMs / 1000)),
  };
  const file = save('sweep.json', out);
  console.log('sweep: ' + allRuns + ' runs, ' + allViolations + ' failures, ' +
    allOps + ' client ops, ' + allCommitted + ' committed entries, ' + wallMs + 'ms');
  for (const p of out.profiles) {
    console.log('  ' + p.name.padEnd(15) + ' failures=' + p.failures +
      ' ops=' + p.clientOpsCompleted + ' committed=' + p.committedEntries +
      ' latency p50/p99=' + p.clientLatencyVirtualMs.p50 + '/' + p.clientLatencyVirtualMs.p99 + ' virtual ms');
  }
  console.log('  wrote ' + file);
  return out;
}

module.exports = { main, PROFILES };

if (require.main === module) main();
