'use strict';
const { runScenario } = require('../src/scenario');
const { figure8, duplicateVote } = require('../src/scripted');
const mutants = require('../src/mutants');
const { save } = require('./util');

// How many seeds it takes before each broken variant trips something, and which
// invariant fires first. A mutant that needs a scripted case rather than random
// search is reported as such rather than quietly excluded.
const SCRIPTED = {
  'commit-across-terms': figure8,
  'forget-vote-on-crash': duplicateVote,
};

function main(seeds) {
  const total = seeds || 40;
  const started = Date.now();
  const rows = [];
  for (const name of mutants.list()) {
    const mutate = mutants.get(name);
    let caught = 0, firstSeed = null;
    const rules = {};
    for (let seed = 1; seed <= total; seed++) {
      const r = runScenario({
        seed: seed, n: 5, duration: 12000, settle: 5000,
        dropRate: 0.05, dupRate: 0.02, latencyMax: 60, faultEvery: 350, mutate: mutate,
      });
      if (!r.ok) {
        caught++;
        if (firstSeed === null) firstSeed = seed;
        for (const v of r.violations) rules[v.rule] = (rules[v.rule] || 0) + 1;
        if (r.convergence.problems.length) rules.convergence = (rules.convergence || 0) + 1;
        if (r.linearizability && !r.linearizability.ok) rules.linearizability = (rules.linearizability || 0) + 1;
      }
    }
    const row = {
      mutant: name, seeds: total, detectedBy: caught > 0 ? 'fuzz' : null,
      seedsDetected: caught, firstSeed: firstSeed,
      detectionRate: Math.round((caught / total) * 1000) / 1000,
      invariantsFired: Object.keys(rules).sort(),
    };
    if (caught === 0 && SCRIPTED[name]) {
      const scripted = SCRIPTED[name](mutate);
      const fired = {};
      for (const v of scripted.violations) fired[v.rule] = true;
      row.detectedBy = 'scripted';
      row.scriptedCase = name === 'commit-across-terms' ? 'figure8' : 'duplicateVote';
      row.invariantsFired = Object.keys(fired).sort();
    }
    rows.push(row);
    console.log('  ' + name.padEnd(24) + ' ' + String(row.detectedBy).padEnd(9) +
      ' rate=' + row.detectionRate + ' firstSeed=' + row.firstSeed +
      ' fired=' + row.invariantsFired.join(','));
  }

  // Control: the real implementation over the same seeds must stay clean.
  let clean = 0;
  for (let seed = 1; seed <= total; seed++) {
    const r = runScenario({
      seed: seed, n: 5, duration: 12000, settle: 5000,
      dropRate: 0.05, dupRate: 0.02, latencyMax: 60, faultEvery: 350,
    });
    if (r.ok) clean++;
  }

  const out = {
    seedsPerMutant: total,
    mutants: rows,
    control: { seeds: total, clean: clean, failures: total - clean },
    allDetected: rows.every(function (r) { return r.detectedBy !== null; }),
    wallMs: Date.now() - started,
  };
  const file = save('mutation.json', out);
  console.log('mutation: ' + rows.length + ' mutants, all detected=' + out.allDetected +
    ', control clean ' + clean + '/' + total + ', ' + out.wallMs + 'ms');
  console.log('  wrote ' + file);
  return out;
}

module.exports = { main };

if (require.main === module) main();
