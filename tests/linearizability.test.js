'use strict';
const { test, ok, eq } = require('./harness');
const lin = require('../src/linearizability');

let counter = 0;
function inv(proc, op) { counter++; return { type: 'invoke', id: 'op' + counter, proc: proc, op: op }; }
function res(ev, value) { return { type: 'ok', id: ev.id, proc: ev.proc, op: ev.op, result: value }; }
function pending(ev) { return { type: 'info', id: ev.id, proc: ev.proc, op: ev.op }; }

test('a plain sequential history is linearizable', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c1', { op: 'get', key: 'x' });
  const r = lin.check([a, res(a, 1), b, res(b, 1)]);
  ok(r.ok);
});

test('a read that misses an already returned write is not linearizable', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c2', { op: 'get', key: 'x' });
  const r = lin.check([a, res(a, 1), b, res(b, null)]);
  ok(!r.ok);
});

test('concurrent operations may be ordered either way', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c2', { op: 'get', key: 'x' });
  // both are in flight at the same time, so reading null is still fine
  const r = lin.check([a, b, res(b, null), res(a, 1)]);
  ok(r.ok);
});

test('a stale read after a settled write is caught even under concurrency', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c2', { op: 'get', key: 'x' });
  const c = inv('c3', { op: 'get', key: 'x' });
  const h = [a, res(a, 1), b, res(b, 1), c, res(c, null)];
  ok(!lin.check(h).ok);
});

test('two successful compare and swaps against the same old value cannot both win', function () {
  const a = inv('c1', { op: 'cas', key: 'x', expect: null, value: 1 });
  const b = inv('c2', { op: 'cas', key: 'x', expect: null, value: 2 });
  const h = [a, b, res(a, true), res(b, true)];
  ok(!lin.check(h).ok);
});

test('one winning and one losing compare and swap is fine', function () {
  const a = inv('c1', { op: 'cas', key: 'x', expect: null, value: 1 });
  const b = inv('c2', { op: 'cas', key: 'x', expect: null, value: 2 });
  const h = [a, b, res(a, true), res(b, false)];
  ok(lin.check(h).ok);
});

test('delete reports whether the key existed', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c1', { op: 'del', key: 'x' });
  const c = inv('c1', { op: 'del', key: 'x' });
  ok(lin.check([a, res(a, 1), b, res(b, true), c, res(c, false)]).ok);
  const d = inv('c1', { op: 'del', key: 'y' });
  ok(!lin.check([d, res(d, true)]).ok);
});

test('an operation that never returned may be treated as having happened', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 7 });
  const b = inv('c2', { op: 'get', key: 'x' });
  ok(lin.check([a, pending(a), b, res(b, 7)]).ok);
});

test('an operation that never returned may also be treated as never happening', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 7 });
  const b = inv('c2', { op: 'get', key: 'x' });
  ok(lin.check([a, pending(a), b, res(b, null)]).ok);
});

test('a pending operation cannot rescue a genuinely bad history', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c2', { op: 'put', key: 'x', value: 9 });
  const c = inv('c3', { op: 'get', key: 'x' });
  // x is definitely 1 by the time c reads, and the pending write is for a
  // different value, so reading 5 is impossible either way
  const h = [a, res(a, 1), b, pending(b), c, res(c, 5)];
  ok(!lin.check(h).ok);
});

test('keys are checked independently', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c1', { op: 'put', key: 'y', value: 2 });
  const c = inv('c2', { op: 'get', key: 'x' });
  const d = inv('c2', { op: 'get', key: 'y' });
  const r = lin.check([a, res(a, 1), b, res(b, 2), c, res(c, 1), d, res(d, 2)]);
  ok(r.ok);
  eq(r.keys, 2);
});

test('a violation on one key fails the whole history', function () {
  const a = inv('c1', { op: 'put', key: 'x', value: 1 });
  const b = inv('c1', { op: 'get', key: 'x' });
  const c = inv('c1', { op: 'get', key: 'y' });
  ok(!lin.check([a, res(a, 1), b, res(b, 1), c, res(c, 99)]).ok);
});

test('a long single client history is checked in close to linear work', function () {
  const h = [];
  for (let i = 0; i < 300; i++) {
    const w = inv('c1', { op: 'put', key: 'x', value: i });
    h.push(w, res(w, i));
    const rd = inv('c1', { op: 'get', key: 'x' });
    h.push(rd, res(rd, i));
  }
  const r = lin.check(h);
  ok(r.ok);
  ok(r.explored < 1500, 'explored ' + r.explored);
});

test('the search reports unknown instead of guessing when it runs out of budget', function () {
  const h = [];
  for (let i = 0; i < 40; i++) {
    const w = inv('c' + i, { op: 'put', key: 'x', value: i });
    h.push(w);
  }
  for (let i = 0; i < 40; i++) h.push({ type: 'ok', id: 'op' + (counter - 39 + i), proc: 'c' + i, op: { op: 'put', key: 'x', value: i }, result: i });
  const r = lin.check(h, { limit: 200 });
  ok(r.unknown || r.ok);
  ok(!(r.ok && r.unknown));
});

test('an empty history is trivially linearizable', function () {
  ok(lin.check([]).ok);
});

test('a response without an invoke is rejected loudly', function () {
  let threw = false;
  try { lin.check([{ type: 'ok', id: 'ghost', proc: 'c1', op: { op: 'get', key: 'x' }, result: null }]); }
  catch (e) { threw = true; }
  ok(threw);
});
