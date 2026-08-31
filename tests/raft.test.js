'use strict';
const { test, ok, eq, deepEq } = require('./harness');
const { RaftNode, FOLLOWER, CANDIDATE, LEADER } = require('../src/raft');
const { Rng } = require('../src/rand');

const IDS = ['n0', 'n1', 'n2', 'n3', 'n4'];
function make(id, opts) {
  return new RaftNode(id || 'n0', IDS, Object.assign({ rng: new Rng(1) }, opts || {}));
}
function entries(node) {
  return node.log.slice(1).map(function (e) { return e.index + ':' + e.term; });
}
function appendMsg(over) {
  return Object.assign({
    type: 'AppendEntries', from: 'n1', to: 'n0', term: 1,
    prevLogIndex: 0, prevLogTerm: 0, entries: [], leaderCommit: 0,
  }, over || {});
}

test('a fresh node is a follower at term 0 with only the sentinel entry', function () {
  const n = make();
  eq(n.state, FOLLOWER);
  eq(n.currentTerm, 0);
  eq(n.log.length, 1);
  eq(n.lastIndex(), 0);
  eq(n.lastTerm(), 0);
});

test('quorum is a strict majority', function () {
  eq(make('n0').quorum, 3);
  eq(new RaftNode('a', ['a', 'b', 'c'], {}).quorum, 2);
  eq(new RaftNode('a', ['a'], {}).quorum, 1);
});

test('an election timeout turns a follower into a candidate at the next term', function () {
  const n = make();
  n.tick(n.electionDeadline + 1);
  eq(n.state, CANDIDATE);
  eq(n.currentTerm, 1);
  eq(n.votedFor, 'n0');
  eq(n.drain().length, 4, 'one RequestVote per peer');
});

test('a single node cluster elects itself', function () {
  const solo = new RaftNode('only', ['only'], { rng: new Rng(1) });
  solo.tick(solo.electionDeadline + 1);
  eq(solo.state, LEADER);
});

test('a candidate becomes leader on a majority of votes', function () {
  const n = make();
  n.becomeCandidate(0);
  n.drain();
  n.handle({ type: 'RequestVoteResp', from: 'n1', to: 'n0', term: 1, granted: true }, 1);
  eq(n.state, CANDIDATE);
  n.handle({ type: 'RequestVoteResp', from: 'n2', to: 'n0', term: 1, granted: true }, 2);
  eq(n.state, LEADER);
});

test('votes from an older election are ignored', function () {
  const n = make();
  n.becomeCandidate(0);
  n.becomeCandidate(1); // timed out and started term 2
  n.drain();
  n.handle({ type: 'RequestVoteResp', from: 'n1', to: 'n0', term: 1, granted: true }, 2);
  n.handle({ type: 'RequestVoteResp', from: 'n2', to: 'n0', term: 1, granted: true }, 3);
  eq(n.state, CANDIDATE, 'stale grants must not add up');
});

test('a duplicated grant from the same peer only counts once', function () {
  const n = make();
  n.becomeCandidate(0);
  n.drain();
  for (let i = 0; i < 5; i++) {
    n.handle({ type: 'RequestVoteResp', from: 'n1', to: 'n0', term: 1, granted: true }, 1);
  }
  eq(n.state, CANDIDATE);
});

test('a higher term in any message forces a step down', function () {
  const n = make();
  n.becomeCandidate(0);
  n.drain();
  n.handle({ type: 'RequestVoteResp', from: 'n1', to: 'n0', term: 9, granted: false }, 1);
  eq(n.state, FOLLOWER);
  eq(n.currentTerm, 9);
  eq(n.votedFor, null);
});

test('a vote is granted once per term and not again', function () {
  const n = make();
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 1, lastLogIndex: 0, lastLogTerm: 0 }, 0);
  eq(n.drain()[0].granted, true);
  n.handle({ type: 'RequestVote', from: 'n2', to: 'n0', term: 1, lastLogIndex: 0, lastLogTerm: 0 }, 1);
  eq(n.drain()[0].granted, false);
});

test('repeating the same vote request is idempotent', function () {
  const n = make();
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 1, lastLogIndex: 0, lastLogTerm: 0 }, 0);
  n.drain();
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 1, lastLogIndex: 0, lastLogTerm: 0 }, 1);
  eq(n.drain()[0].granted, true);
});

test('a vote request from an old term is refused', function () {
  const n = make();
  n.currentTerm = 5;
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 3, lastLogIndex: 9, lastLogTerm: 9 }, 0);
  const reply = n.drain()[0];
  eq(reply.granted, false);
  eq(reply.term, 5);
});

test('a candidate with a shorter last term loses the vote', function () {
  const n = make();
  n.log.push({ index: 1, term: 4, cmd: null });
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 5, lastLogIndex: 9, lastLogTerm: 3 }, 0);
  eq(n.drain()[0].granted, false, 'a longer log at a lower term is still behind');
});

test('a candidate with an equal last term but a shorter log loses the vote', function () {
  const n = make();
  n.log.push({ index: 1, term: 2, cmd: null });
  n.log.push({ index: 2, term: 2, cmd: null });
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 3, lastLogIndex: 1, lastLogTerm: 2 }, 0);
  eq(n.drain()[0].granted, false);
});

test('an equal log is up to date enough to win the vote', function () {
  const n = make();
  n.log.push({ index: 1, term: 2, cmd: null });
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 3, lastLogIndex: 1, lastLogTerm: 2 }, 0);
  eq(n.drain()[0].granted, true);
});

test('refusing a vote does not reset the election timer', function () {
  const n = make();
  n.currentTerm = 5;
  const deadline = n.electionDeadline;
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 3, lastLogIndex: 0, lastLogTerm: 0 }, 40);
  eq(n.electionDeadline, deadline, 'otherwise a spammy stale peer could stall an election forever');
});

test('granting a vote does reset the election timer', function () {
  const n = make();
  const deadline = n.electionDeadline;
  n.handle({ type: 'RequestVote', from: 'n1', to: 'n0', term: 1, lastLogIndex: 0, lastLogTerm: 0 }, 40);
  ok(n.electionDeadline > deadline);
});

test('a new leader initialises nextIndex past its own log', function () {
  const n = make();
  n.log.push({ index: 1, term: 0, cmd: null });
  n.becomeCandidate(0);
  n.votesGranted = new Set(['n0', 'n1', 'n2']);
  n.becomeLeader(0);
  for (const p of n.peers) {
    eq(n.nextIndex.get(p), 2);
    eq(n.matchIndex.get(p), 0);
  }
});

test('only a leader accepts a proposal, others hand back a hint', function () {
  const n = make();
  n.leaderId = 'n2';
  const res = n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  eq(res.ok, false);
  eq(res.leaderHint, 'n2');
});

test('a proposal appends at the leader term and fans out', function () {
  const n = make();
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  const res = n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  eq(res.ok, true);
  eq(res.index, 1);
  eq(res.term, 1);
  eq(n.drain().length, 4);
});

test('a follower rejects AppendEntries from an older term', function () {
  const n = make();
  n.currentTerm = 4;
  n.handle(appendMsg({ term: 2 }), 0);
  const reply = n.drain()[0];
  eq(reply.success, false);
  eq(reply.term, 4);
});

test('a follower rejects a prevLogIndex it does not have', function () {
  const n = make();
  n.handle(appendMsg({ term: 1, prevLogIndex: 5, prevLogTerm: 1 }), 0);
  eq(n.drain()[0].success, false);
});

test('a follower rejects a prevLogTerm mismatch', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: null });
  n.handle(appendMsg({ term: 2, prevLogIndex: 1, prevLogTerm: 2 }), 0);
  eq(n.drain()[0].success, false);
});

test('a matching AppendEntries appends and acknowledges', function () {
  const n = make();
  n.handle(appendMsg({ entries: [{ index: 1, term: 1, cmd: { op: 'put', key: 'a', value: 1 } }] }), 0);
  const reply = n.drain()[0];
  eq(reply.success, true);
  eq(reply.matchIndex, 1);
  deepEq(entries(n), ['1:1']);
});

test('a conflicting entry truncates everything after it', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'a' });
  n.log.push({ index: 2, term: 1, cmd: 'b' });
  n.log.push({ index: 3, term: 1, cmd: 'c' });
  n.handle(appendMsg({ term: 2, prevLogIndex: 1, prevLogTerm: 1, entries: [{ index: 2, term: 2, cmd: 'B' }] }), 0);
  deepEq(entries(n), ['1:1', '2:2']);
});

test('a duplicated AppendEntries does not truncate anything', function () {
  const n = make();
  const msg = appendMsg({ entries: [
    { index: 1, term: 1, cmd: 'a' }, { index: 2, term: 1, cmd: 'b' },
  ] });
  n.handle(msg, 0);
  n.drain();
  n.handle(appendMsg({ entries: [{ index: 1, term: 1, cmd: 'a' }] }), 1);
  deepEq(entries(n), ['1:1', '2:1'], 'the second entry must survive the replay');
});

test('an empty AppendEntries acknowledges at prevLogIndex', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'a' });
  n.handle(appendMsg({ term: 1, prevLogIndex: 1, prevLogTerm: 1 }), 0);
  eq(n.drain()[0].matchIndex, 1);
});

test('a candidate that hears a valid leader steps back to follower', function () {
  const n = make();
  n.becomeCandidate(0);
  n.drain();
  n.handle(appendMsg({ term: n.currentTerm }), 1);
  eq(n.state, FOLLOWER);
  eq(n.leaderId, 'n1');
});

test('leaderCommit advances the follower commit index but not past its log', function () {
  const n = make();
  n.handle(appendMsg({ entries: [{ index: 1, term: 1, cmd: 'a' }], leaderCommit: 9 }), 0);
  eq(n.commitIndex, 1);
});

test('a delayed AppendEntries cannot drag commitIndex backwards', function () {
  const n = make();
  n.handle(appendMsg({ entries: [
    { index: 1, term: 1, cmd: 'a' }, { index: 2, term: 1, cmd: 'b' }, { index: 3, term: 1, cmd: 'c' },
  ], leaderCommit: 3 }), 0);
  eq(n.commitIndex, 3);
  n.drain();
  // A straggler that only covers index 1, but still carries a high leaderCommit.
  n.handle(appendMsg({ term: 1, prevLogIndex: 1, prevLogTerm: 1, entries: [], leaderCommit: 5 }), 1);
  eq(n.commitIndex, 3, 'this regression is exactly what the fuzzer found first');
});

test('committed entries are applied in order exactly once', function () {
  const n = make();
  n.handle(appendMsg({ entries: [
    { index: 1, term: 1, cmd: 'a' }, { index: 2, term: 1, cmd: 'b' },
  ], leaderCommit: 2 }), 0);
  const applied = n.takeApplied();
  deepEq(applied.map(function (e) { return e.cmd; }), ['a', 'b']);
  deepEq(n.takeApplied(), []);
});

test('a leader commits an entry from its own term once a majority has it', function () {
  const n = make();
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  n.drain();
  eq(n.commitIndex, 0);
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: 1, success: true, matchIndex: 1 }, 1);
  eq(n.commitIndex, 0, 'two of five is not a majority');
  n.handle({ type: 'AppendEntriesResp', from: 'n2', to: 'n0', term: 1, success: true, matchIndex: 1 }, 2);
  eq(n.commitIndex, 1);
});

test('a leader will not commit an entry from an earlier term by counting replicas', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'old' });
  n.currentTerm = 3;
  n.becomeCandidate(0); // term 4
  n.becomeLeader(0);
  n.drain();
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: 4, success: true, matchIndex: 1 }, 1);
  n.handle({ type: 'AppendEntriesResp', from: 'n2', to: 'n0', term: 4, success: true, matchIndex: 1 }, 2);
  eq(n.commitIndex, 0, 'figure 8: the term check is what keeps this uncommitted');
});

test('a new entry in the leader term commits the older ones with it', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'old' });
  n.currentTerm = 3;
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  n.drain();
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: 4, success: true, matchIndex: 2 }, 1);
  n.handle({ type: 'AppendEntriesResp', from: 'n2', to: 'n0', term: 4, success: true, matchIndex: 2 }, 2);
  eq(n.commitIndex, 2);
  eq(n.takeApplied().length, 2);
});

test('a failed AppendEntries walks nextIndex back and retries', function () {
  const n = make();
  for (let i = 1; i <= 5; i++) n.log.push({ index: i, term: 1, cmd: 'e' + i });
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  eq(n.nextIndex.get('n1'), 6);
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: n.currentTerm, success: false, matchIndex: 0 }, 1);
  eq(n.nextIndex.get('n1'), 5);
  eq(n.drain().length, 1, 'and it retries straight away');
});

test('nextIndex never walks below one', function () {
  const n = make();
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  for (let i = 0; i < 10; i++) {
    n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: n.currentTerm, success: false, matchIndex: 0 }, i);
    n.drain();
  }
  eq(n.nextIndex.get('n1'), 1);
});

test('matchIndex never moves backwards on a reordered response', function () {
  const n = make();
  for (let i = 1; i <= 3; i++) n.log.push({ index: i, term: 1, cmd: 'e' + i });
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: n.currentTerm, success: true, matchIndex: 3 }, 1);
  n.drain();
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: n.currentTerm, success: true, matchIndex: 1 }, 2);
  eq(n.matchIndex.get('n1'), 3);
});

test('responses addressed to an old term are ignored by the leader', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'a' });
  n.currentTerm = 5;
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  n.handle({ type: 'AppendEntriesResp', from: 'n1', to: 'n0', term: 3, success: true, matchIndex: 1 }, 1);
  eq(n.matchIndex.get('n1'), 0);
});

test('a leader heartbeats on schedule and not before', function () {
  const n = make(null, { heartbeatInterval: 50 });
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.drain();
  n.tick(0);
  eq(n.drain().length, 4);
  n.tick(20);
  eq(n.drain().length, 0);
  n.tick(50);
  eq(n.drain().length, 4);
});

test('persistent state round trips and volatile state is dropped', function () {
  const n = make();
  n.becomeCandidate(0);
  n.becomeLeader(0);
  n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  n.commitIndex = 1;
  n.applyCommitted();
  const saved = JSON.parse(JSON.stringify(n.persistentState()));

  const revived = make();
  revived.restore(saved);
  eq(revived.currentTerm, n.currentTerm);
  eq(revived.votedFor, 'n0');
  deepEq(entries(revived), entries(n));
  eq(revived.state, FOLLOWER);
  eq(revived.commitIndex, 0);
  eq(revived.lastApplied, 0);
});

test('restoring a snapshot does not alias the original log', function () {
  const n = make();
  n.log.push({ index: 1, term: 1, cmd: 'a' });
  const saved = n.persistentState();
  const revived = make();
  revived.restore(saved);
  revived.log.push({ index: 2, term: 1, cmd: 'b' });
  eq(n.log.length, 2);
  eq(saved.log.length, 2);
});

test('every persistent change is announced', function () {
  const n = make();
  const before = n.persistOps;
  n.becomeCandidate(0);
  ok(n.persistOps > before, 'term and vote both changed');
  const mid = n.persistOps;
  n.becomeLeader(0);
  n.propose({ op: 'put', key: 'a', value: 1 }, 0);
  ok(n.persistOps > mid, 'appending to the log is persistent too');
});

test('election timeouts are spread across the configured window', function () {
  const seen = new Set();
  for (let s = 0; s < 60; s++) {
    const n = new RaftNode('n0', IDS, { rng: new Rng(s), electionTimeoutMin: 150, electionTimeoutMax: 300 });
    ok(n.electionDeadline >= 150 && n.electionDeadline <= 300);
    seen.add(n.electionDeadline);
  }
  ok(seen.size > 30, 'otherwise split votes would repeat forever, saw ' + seen.size);
});

test('an unknown message type is loud rather than silent', function () {
  const n = make();
  let threw = false;
  try { n.handle({ type: 'Gossip', from: 'n1', to: 'n0', term: 0 }, 0); } catch (e) { threw = true; }
  ok(threw);
});
