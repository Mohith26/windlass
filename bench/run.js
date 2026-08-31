'use strict';
const sweep = require('./sweep');
const mutation = require('./mutation');
const checker = require('./checker');
const latency = require('./latency');
const { save } = require('./util');

function main() {
  const started = Date.now();
  console.log('checker overhead');
  const c = checker.main();
  console.log('latency');
  const l = latency.main();
  console.log('mutation coverage');
  const m = mutation.main();
  console.log('randomised sweep');
  const s = sweep.main();

  const summary = {
    generatedBy: 'bench/run.js',
    totalWallMs: Date.now() - started,
    sweep: s.totals,
    mutationAllDetected: m.allDetected,
    mutationControlClean: m.control,
    checkerSpeedupAtLargest: c.points[c.points.length - 1].speedup,
    electionP50VirtualMs: l.electionAfterLeaderCrashVirtualMs['5'].p50,
  };
  save('summary.json', summary);
  console.log('');
  console.log('total ' + summary.totalWallMs + 'ms');
  return summary;
}

module.exports = { main };

if (require.main === module) main();
