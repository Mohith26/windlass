'use strict';
const { Cluster } = require('../src/sim');
const { ClientPool } = require('../src/client');
const { InvariantChecker } = require('../src/invariants');
const { save } = require('./util');

// What the invariant checker costs. The first version rescanned every log from
// index zero on every node step, which made the checker the bottleneck by a
// wide margin. Both modes are still in the code so this comparison is a real
// measurement rather than a remembered number.
function once(mode, virtualMs) {
  const c = new Cluster({ n: 5, seed: 3, dropRate: 0.02, dupRate: 0.01 });
  let checker = null;
  if (mode !== 'off') checker = new InvariantChecker({ incremental: mode === 'incremental' }).attach(c);
  const pool = new ClientPool(c, { clients: 3, keys: ['a', 'b', 'c'], seed: 99 });
  const t0 = Date.now();
  while (c.now < virtualMs) { if (!c.step()) c.runFor(10); pool.pump(); }
  const ms = Date.now() - t0;
  return {
    mode: mode, wallMs: ms, steps: c.stats.steps,
    committed: c.nodes.get('n0').commitIndex,
    checks: checker ? checker.checks : 0,
    rescans: checker ? checker.rescans : 0,
    violations: checker ? checker.violations.length : 0,
  };
}

function main() {
  const started = Date.now();
  const points = [];
  for (const virtualMs of [1500, 3000, 4500, 6000]) {
    const off = once('off', virtualMs);
    const rescan = once('rescan', virtualMs);
    const inc = once('incremental', virtualMs);
    points.push({
      virtualMs: virtualMs, committedEntries: off.committed, simulatorSteps: off.steps,
      noCheckerMs: off.wallMs, rescanMs: rescan.wallMs, incrementalMs: inc.wallMs,
      speedup: rescan.wallMs > 0 ? Math.round((rescan.wallMs / Math.max(1, inc.wallMs)) * 10) / 10 : null,
      incrementalOverhead: Math.round((inc.wallMs / Math.max(1, off.wallMs)) * 10) / 10,
      checks: inc.checks, truncationRescans: inc.rescans,
      violations: inc.violations,
    });
    console.log('  ' + virtualMs + ' virtual ms, ' + off.committed + ' committed: none=' +
      off.wallMs + 'ms rescan=' + rescan.wallMs + 'ms incremental=' + inc.wallMs + 'ms');
  }
  const out = { points: points, wallMs: Date.now() - started };
  const file = save('checker-overhead.json', out);
  console.log('  wrote ' + file);
  return out;
}

module.exports = { main };

if (require.main === module) main();
