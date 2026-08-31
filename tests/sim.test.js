'use strict';
const { test, ok, eq, deepEq } = require('./harness');
const { Cluster, MinHeap } = require('../src/sim');
const { LEADER } = require('../src/raft');
const { InvariantChecker } = require('../src/invariants');

test('the heap pops in time order', function () {
  const h = new MinHeap();
  for (const at of [5, 1, 9, 3, 7, 2]) h.push({ at: at, seq: at });
  const out = [];
  while (h.size > 0) out.push(h.pop().at);
  deepEq(out, [1, 2, 3, 5, 7, 9]);
});

test('the heap breaks ties by insertion order', function () {
  const h = new MinHeap();
  for (let i = 0; i < 20; i++) h.push({ at: 10, seq: i });
  const out = [];
  while (h.size > 0) out.push(h.pop().seq);
  deepEq(out, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]);
});

test('a healthy cluster elects exactly one leader', function () {
  const c = new Cluster({ n: 5, seed: 21 });
  c.runFor(1500);
  eq(c.leaders().length, 1);
});

test('the same seed replays exactly', function () {
  const a = new Cluster({ n: 5, seed: 77, dropRate: 0.1, dupRate: 0.05 });
  const b = new Cluster({ n: 5, seed: 77, dropRate: 0.1, dupRate: 0.05 });
  a.runFor(4000);
  b.runFor(4000);
  deepEq(a.stats, b.stats);
  deepEq(a.leaders(), b.leaders());
  for (const id of a.ids) {
    eq(a.nodes.get(id).currentTerm, b.nodes.get(id).currentTerm);
    eq(a.nodes.get(id).commitIndex, b.nodes.get(id).commitIndex);
  }
});

test('a different seed produces a different run', function () {
  const a = new Cluster({ n: 5, seed: 1 });
  const b = new Cluster({ n: 5, seed: 2 });
  a.runFor(3000);
  b.runFor(3000);
  ok(JSON.stringify(a.stats) !== JSON.stringify(b.stats));
});

test('entries replicate to every node', function () {
  const c = new Cluster({ n: 5, seed: 5 });
  c.runFor(1000);
  const leader = c.liveLeader();
  ok(leader !== null);
  for (let i = 0; i < 10; i++) {
    const before = leader.state;
    leader.propose({ op: 'put', key: 'k', value: i }, c.now);
    c.afterNodeStep(leader, before);
    c.runFor(60);
  }
  c.runFor(500);
  const want = c.machines.get(leader.id).digest();
  for (const id of c.ids) eq(c.machines.get(id).digest(), want);
});

test('a partition stops traffic in both directions', function () {
  const c = new Cluster({ n: 5, seed: 8 });
  c.partition([['n0', 'n1'], ['n2', 'n3', 'n4']]);
  ok(c.reachable('n0', 'n1'));
  ok(!c.reachable('n0', 'n2'));
  ok(!c.reachable('n2', 'n0'));
  c.heal();
  ok(c.reachable('n0', 'n2'));
});

test('a minority partition cannot commit anything', function () {
  // Note that a leader stranded in a minority does not step down. Plain Raft has
  // no lease or quorum check, so it keeps calling itself leader until it hears a
  // higher term. What it cannot do is commit, and that is the real property.
  const c = new Cluster({ n: 5, seed: 12 });
  c.runFor(1200);
  c.partition([['n0', 'n1'], ['n2', 'n3', 'n4']]);
  c.runFor(1000);
  const before = ['n0', 'n1'].map(function (id) { return c.nodes.get(id).commitIndex; });
  for (const id of ['n0', 'n1']) {
    const node = c.nodes.get(id);
    if (node.state === LEADER) {
      const was = node.state;
      node.propose({ op: 'put', key: 'stranded', value: 1 }, c.now);
      c.afterNodeStep(node, was);
    }
  }
  c.runFor(4000);
  const after = ['n0', 'n1'].map(function (id) { return c.nodes.get(id).commitIndex; });
  deepEq(after, before, 'nothing may commit on the minority side');
  eq(c.machines.get('n0').map.has('stranded'), false);
});

test('the majority side keeps a leader through a partition', function () {
  const c = new Cluster({ n: 5, seed: 13 });
  c.runFor(1200);
  c.partition([['n0', 'n1'], ['n2', 'n3', 'n4']]);
  c.runFor(4000);
  const majority = ['n2', 'n3', 'n4'].filter(function (id) { return c.nodes.get(id).state === LEADER; });
  eq(majority.length, 1);
});

test('a crashed node sends and receives nothing', function () {
  const c = new Cluster({ n: 5, seed: 14 });
  c.runFor(1000);
  c.crash('n0');
  ok(!c.reachable('n0', 'n1'));
  const sent = c.stats.messagesSent;
  c.runFor(500);
  ok(c.stats.messagesSent > sent, 'the rest of the cluster carries on');
});

test('a restart keeps the disk and drops everything else', function () {
  const c = new Cluster({ n: 5, seed: 15 });
  c.runFor(1500);
  const leader = c.liveLeader();
  for (let i = 0; i < 5; i++) {
    const before = leader.state;
    leader.propose({ op: 'put', key: 'k', value: i }, c.now);
    c.afterNodeStep(leader, before);
    c.runFor(60);
  }
  c.runFor(400);
  const victim = c.ids.find(function (id) { return id !== leader.id; });
  const termBefore = c.nodes.get(victim).currentTerm;
  const logBefore = c.nodes.get(victim).log.length;
  c.crash(victim);
  c.restart(victim);
  const revived = c.nodes.get(victim);
  eq(revived.currentTerm, termBefore);
  eq(revived.log.length, logBefore);
  eq(revived.commitIndex, 0, 'commit index is volatile');
  eq(c.machines.get(victim).map.size, 0, 'and so is the state machine');
});

test('a restarted node catches its state machine back up', function () {
  const c = new Cluster({ n: 5, seed: 16 });
  c.runFor(1500);
  const leader = c.liveLeader();
  for (let i = 0; i < 8; i++) {
    const before = leader.state;
    leader.propose({ op: 'put', key: 'k' + i, value: i }, c.now);
    c.afterNodeStep(leader, before);
    c.runFor(60);
  }
  c.runFor(400);
  const victim = c.ids.find(function (id) { return id !== leader.id; });
  const want = c.machines.get(victim).digest();
  c.crash(victim);
  c.restart(victim);
  c.runFor(2000);
  eq(c.machines.get(victim).digest(), want);
});

test('dropped messages are counted and the cluster still converges', function () {
  const c = new Cluster({ n: 5, seed: 17, dropRate: 0.25 });
  c.runFor(6000);
  ok(c.stats.messagesDropped > 0);
  eq(c.leaders().length, 1);
});

test('duplicated messages do not corrupt anything', function () {
  const c = new Cluster({ n: 5, seed: 18, dupRate: 0.3 });
  const checker = new InvariantChecker().attach(c);
  c.runFor(1000);
  const leader = c.liveLeader();
  for (let i = 0; i < 20; i++) {
    const before = leader.state;
    leader.propose({ op: 'put', key: 'k' + (i % 3), value: i }, c.now);
    c.afterNodeStep(leader, before);
    c.runFor(40);
  }
  c.runFor(1000);
  ok(c.stats.messagesDuplicated > 0);
  deepEq(checker.finalCheck(c), []);
});

test('liveLeader ignores a leader that cannot reach a quorum', function () {
  const c = new Cluster({ n: 5, seed: 19 });
  c.runFor(1200);
  const leader = c.liveLeader();
  ok(leader !== null);
  c.isolate(leader.id);
  eq(c.liveLeader(), null);
});

test('persistence happens before anything is sent', function () {
  const c = new Cluster({ n: 5, seed: 20 });
  c.runFor(1000);
  for (const id of c.ids) {
    const node = c.nodes.get(id);
    const disk = c.disk.get(id);
    eq(disk.currentTerm, node.currentTerm);
    eq(disk.votedFor, node.votedFor);
    eq(disk.log.length, node.log.length);
  }
});

test('the simulator stops when there is nothing left to do', function () {
  const c = new Cluster({ n: 3, seed: 22 });
  for (const id of c.ids) c.crash(id);
  eq(c.step(), false);
});
