'use strict';

const { Cluster } = require('./sim');
const { InvariantChecker } = require('./invariants');
const { LEADER } = require('./raft');

// Two hand built histories. Random fault injection never reproduced either of
// them in tens of thousands of runs, because both need a very particular
// ordering of elections and crashes, so they are scripted instead.
//
// Both are written so that the same script runs against the real implementation
// and against a mutant, and the only difference in the outcome is whether the
// invariant checker fires.

// Elections and heartbeats are pushed effectively to infinity so that nothing
// happens unless the script asks for it.
const FROZEN = { electionTimeoutMin: 1e9, electionTimeoutMax: 1e9, heartbeatInterval: 1e9 };

function build(n, mutate) {
  const cluster = new Cluster({
    n: n, seed: 1, latencyMin: 1, latencyMax: 1, dropRate: 0, dupRate: 0,
    raft: FROZEN, mutate: mutate || null,
  });
  const checker = new InvariantChecker().attach(cluster);
  return { cluster: cluster, checker: checker };
}

function kick(cluster, id) {
  const node = cluster.nodes.get(id);
  const before = node.state;
  node.becomeCandidate(cluster.now);
  cluster.afterNodeStep(node, before);
}

function propose(cluster, id, cmd) {
  const node = cluster.nodes.get(id);
  const before = node.state;
  const res = node.propose(cmd, cluster.now);
  cluster.afterNodeStep(node, before);
  return res;
}

// Keep calling elections on one node until it wins. Each call is one election
// timeout firing, which is exactly what a node isolated from the leader does.
function electUntilLeader(cluster, id, attempts) {
  for (let i = 0; i < (attempts || 4); i++) {
    kick(cluster, id);
    cluster.runFor(60);
    if (cluster.nodes.get(id).state === LEADER) return true;
  }
  return false;
}

// Figure 8 of the Raft paper. An entry from an old term is replicated onto a
// majority by a later leader. Counting replicas alone would call it committed,
// and then a node that still holds a higher term entry at that index can win the
// next election and overwrite it.
function figure8(mutate) {
  const { cluster, checker } = build(5, mutate);
  const trace = [];
  const note = function (s) { trace.push(cluster.now + ' ' + s); };

  // n0 leads term 1 with everyone reachable.
  if (!electUntilLeader(cluster, 'n0', 2)) throw new Error('n0 never became leader');
  note('n0 leader term ' + cluster.nodes.get('n0').currentTerm);

  // Entry A lands on n0 and n1 only, well short of a majority.
  cluster.partition([['n0', 'n1'], ['n2', 'n3', 'n4']]);
  propose(cluster, 'n0', { op: 'put', key: 'x', value: 'A' });
  cluster.runFor(80);
  note('A at index 1 on n0,n1; n0 commitIndex=' + cluster.nodes.get('n0').commitIndex);

  // n0 disappears. n2 and n3 have empty logs so they will vote for anyone; n1
  // will not, because its log is ahead.
  cluster.crash('n0');
  cluster.partition([['n1', 'n2', 'n3', 'n4']]);
  if (!electUntilLeader(cluster, 'n4', 4)) throw new Error('n4 never became leader');
  note('n4 leader term ' + cluster.nodes.get('n4').currentTerm);

  // Entry B takes the same index on n4 alone, at a higher term.
  cluster.isolate('n4');
  propose(cluster, 'n4', { op: 'put', key: 'x', value: 'B' });
  cluster.runFor(40);
  note('B at index 1 on n4 only, term ' + cluster.nodes.get('n4').log[1].term);

  // n4 drops out, n0 comes back and wins again, then pushes A onto n2. A is now
  // on three of five nodes but it belongs to an older term.
  cluster.crash('n4');
  cluster.restart('n0');
  cluster.partition([['n0', 'n1', 'n2', 'n3']]);
  if (!electUntilLeader(cluster, 'n0', 5)) throw new Error('n0 never regained leadership');
  cluster.runFor(120);
  const commitAfterMajority = cluster.nodes.get('n0').commitIndex;
  note('A replicated to a majority; n0 commitIndex=' + commitAfterMajority);

  // n0 leaves for good and n4 returns. Its higher term entry at index 1 wins the
  // vote and overwrites A everywhere.
  cluster.crash('n0');
  cluster.restart('n4');
  cluster.partition([['n1', 'n2', 'n3', 'n4']]);
  if (!electUntilLeader(cluster, 'n4', 6)) throw new Error('n4 never regained leadership');
  propose(cluster, 'n4', { op: 'put', key: 'y', value: 'C' });
  cluster.runFor(200);
  note('n4 leader term ' + cluster.nodes.get('n4').currentTerm +
    '; index 1 now term ' + cluster.nodes.get('n1').log[1].term);

  const violations = checker.finalCheck(cluster);
  return { cluster: cluster, checker: checker, violations: violations, trace: trace, commitAfterMajority: commitAfterMajority };
}

// A node votes, crashes, and comes back having forgotten the vote. It votes a
// second time in the same term and two different candidates both reach a
// majority.
function duplicateVote(mutate) {
  const { cluster, checker } = build(5, mutate);
  const trace = [];
  const note = function (s) { trace.push(cluster.now + ' ' + s); };

  // n0 campaigns and is only allowed to reach n1 and n2.
  cluster.partition([['n0', 'n1', 'n2'], ['n3', 'n4']]);
  kick(cluster, 'n0');
  cluster.runFor(60);
  note('n0 state=' + cluster.nodes.get('n0').state + ' term=' + cluster.nodes.get('n0').currentTerm);

  // n2 restarts. Its term survives, and its vote should too.
  cluster.crash('n2');
  cluster.restart('n2');
  const votedForAfterRestart = cluster.nodes.get('n2').votedFor;
  note('n2 restarted, votedFor=' + votedForAfterRestart);

  // n4 campaigns in the same term against n2 and n3.
  cluster.partition([['n0', 'n1'], ['n2', 'n3', 'n4']]);
  kick(cluster, 'n4');
  cluster.runFor(60);
  note('n4 state=' + cluster.nodes.get('n4').state + ' term=' + cluster.nodes.get('n4').currentTerm);

  const violations = checker.finalCheck(cluster);
  const terms = {};
  for (const id of cluster.ids) {
    const nd = cluster.nodes.get(id);
    if (nd.state === LEADER) {
      if (!terms[nd.currentTerm]) terms[nd.currentTerm] = [];
      terms[nd.currentTerm].push(id);
    }
  }
  return {
    cluster: cluster, checker: checker, violations: violations, trace: trace,
    leadersByTerm: terms, votedForAfterRestart: votedForAfterRestart,
  };
}

module.exports = { figure8, duplicateVote, build, kick, propose, electUntilLeader, FROZEN };
