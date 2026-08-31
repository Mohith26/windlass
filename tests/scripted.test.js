'use strict';
const { test, ok, eq } = require('./harness');
const { figure8, duplicateVote } = require('../src/scripted');
const mutants = require('../src/mutants');

function rules(violations) {
  const set = {};
  for (const v of violations) set[v.rule] = true;
  return Object.keys(set).sort();
}

test('figure 8 leaves the old term entry uncommitted on the real implementation', function () {
  const r = figure8(null);
  eq(r.commitAfterMajority, 0, 'a majority holding it is not enough on its own');
  eq(r.violations.length, 0);
});

test('figure 8 commits the old term entry once the term check is removed', function () {
  const r = figure8(mutants.get('commit-across-terms'));
  eq(r.commitAfterMajority, 1);
  ok(r.violations.length > 0);
});

test('the lost figure 8 entry shows up as a leader completeness violation', function () {
  const r = figure8(mutants.get('commit-across-terms'));
  ok(rules(r.violations).indexOf('leader-completeness') >= 0, rules(r.violations).join(','));
});

test('the lost figure 8 entry also shows up at the state machine', function () {
  const r = figure8(mutants.get('commit-across-terms'));
  ok(rules(r.violations).indexOf('state-machine-safety') >= 0, rules(r.violations).join(','));
});

test('a restart keeps the vote on the real implementation, so only one leader wins', function () {
  const r = duplicateVote(null);
  eq(r.votedForAfterRestart, 'n0');
  eq(Object.keys(r.leadersByTerm).length, 1);
  eq(r.leadersByTerm['1'].length, 1);
  eq(r.violations.length, 0);
});

test('losing votedFor across a restart produces two leaders in one term', function () {
  const r = duplicateVote(mutants.get('forget-vote-on-crash'));
  eq(r.votedForAfterRestart, null);
  eq(r.leadersByTerm['1'].length, 2);
  ok(rules(r.violations).indexOf('election-safety') >= 0);
});
