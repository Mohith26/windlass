'use strict';
const { test, ok, eq, deepEq, throws } = require('./harness');
const { KvStore } = require('../src/kv');

test('put then get returns the value', function () {
  const kv = new KvStore();
  eq(kv.apply({ op: 'put', key: 'a', value: 1 }).value, 1);
  eq(kv.apply({ op: 'get', key: 'a' }).value, 1);
});

test('get on a missing key is null', function () {
  const kv = new KvStore();
  eq(kv.apply({ op: 'get', key: 'nope' }).value, null);
});

test('del reports whether the key was there', function () {
  const kv = new KvStore();
  kv.apply({ op: 'put', key: 'a', value: 1 });
  eq(kv.apply({ op: 'del', key: 'a' }).value, true);
  eq(kv.apply({ op: 'del', key: 'a' }).value, false);
  eq(kv.apply({ op: 'get', key: 'a' }).value, null);
});

test('cas only writes when the expected value matches', function () {
  const kv = new KvStore();
  eq(kv.apply({ op: 'cas', key: 'a', expect: null, value: 5 }).value, true);
  eq(kv.apply({ op: 'cas', key: 'a', expect: 4, value: 9 }).value, false);
  eq(kv.apply({ op: 'get', key: 'a' }).value, 5);
});

test('a null command is a no-op', function () {
  const kv = new KvStore();
  eq(kv.apply(null), null);
  eq(kv.applied, 1);
});

test('unknown commands throw rather than silently pass', function () {
  const kv = new KvStore();
  throws(function () { kv.apply({ op: 'increment', key: 'a' }); });
});

test('a repeated sequence number returns the cached result and does not reapply', function () {
  const kv = new KvStore();
  const first = kv.apply({ cid: 'c1', seq: 1, op: 'del', key: 'missing' });
  const second = kv.apply({ cid: 'c1', seq: 1, op: 'del', key: 'missing' });
  eq(first.value, false);
  eq(second.value, false);
  eq(kv.deduped, 1);
});

test('a retried put does not double apply', function () {
  const kv = new KvStore();
  kv.apply({ cid: 'c1', seq: 1, op: 'put', key: 'a', value: 1 });
  kv.apply({ cid: 'c1', seq: 2, op: 'cas', key: 'a', expect: 1, value: 2 });
  const replay = kv.apply({ cid: 'c1', seq: 2, op: 'cas', key: 'a', expect: 1, value: 2 });
  eq(replay.value, true, 'the cached answer, not a fresh failing cas');
  eq(kv.apply({ op: 'get', key: 'a' }).value, 2);
});

test('a straggler with an older sequence number is dropped, not replayed', function () {
  const kv = new KvStore();
  kv.apply({ cid: 'c1', seq: 5, op: 'put', key: 'a', value: 'new' });
  const stale = kv.apply({ cid: 'c1', seq: 4, op: 'put', key: 'a', value: 'old' });
  ok(stale.stale === true);
  eq(kv.apply({ op: 'get', key: 'a' }).value, 'new');
});

test('sessions are per client', function () {
  const kv = new KvStore();
  kv.apply({ cid: 'c1', seq: 1, op: 'put', key: 'a', value: 1 });
  const other = kv.apply({ cid: 'c2', seq: 1, op: 'put', key: 'b', value: 2 });
  eq(other.value, 2);
  eq(kv.deduped, 0);
});

test('digest is order independent', function () {
  const a = new KvStore(), b = new KvStore();
  a.apply({ op: 'put', key: 'x', value: 1 });
  a.apply({ op: 'put', key: 'y', value: 2 });
  b.apply({ op: 'put', key: 'y', value: 2 });
  b.apply({ op: 'put', key: 'x', value: 1 });
  eq(a.digest(), b.digest());
});

test('digest reflects the session table as well as the data', function () {
  const a = new KvStore(), b = new KvStore();
  a.apply({ cid: 'c1', seq: 1, op: 'put', key: 'x', value: 1 });
  b.apply({ op: 'put', key: 'x', value: 1 });
  ok(a.digest() !== b.digest());
});

test('toObject is sorted and plain', function () {
  const kv = new KvStore();
  kv.apply({ op: 'put', key: 'b', value: 2 });
  kv.apply({ op: 'put', key: 'a', value: 1 });
  deepEq(Object.keys(kv.toObject()), ['a', 'b']);
});
