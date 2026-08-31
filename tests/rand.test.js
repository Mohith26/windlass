'use strict';
const { test, ok, eq, deepEq } = require('./harness');
const { Rng } = require('../src/rand');

test('same seed gives the same stream', function () {
  const a = new Rng(12345), b = new Rng(12345);
  for (let i = 0; i < 200; i++) eq(a.next(), b.next());
});

test('different seeds diverge immediately', function () {
  const a = new Rng(1), b = new Rng(2);
  ok(a.next() !== b.next());
});

test('nearby seeds do not produce nearby first values', function () {
  const firsts = [];
  for (let s = 1; s <= 8; s++) firsts.push(new Rng(s).float());
  for (let i = 1; i < firsts.length; i++) ok(Math.abs(firsts[i] - firsts[i - 1]) > 0.01);
});

test('float stays inside [0, 1)', function () {
  const r = new Rng(7);
  for (let i = 0; i < 5000; i++) {
    const v = r.float();
    ok(v >= 0 && v < 1);
  }
});

test('int(n) covers every bucket and never leaves the range', function () {
  const r = new Rng(9);
  const counts = new Array(6).fill(0);
  for (let i = 0; i < 60000; i++) {
    const v = r.int(6);
    ok(v >= 0 && v < 6);
    counts[v]++;
  }
  for (const c of counts) ok(c > 8000 && c < 12000, 'bucket ' + c);
});

test('range is inclusive at both ends', function () {
  const r = new Rng(11);
  let sawLo = false, sawHi = false;
  for (let i = 0; i < 4000; i++) {
    const v = r.range(3, 7);
    ok(v >= 3 && v <= 7);
    if (v === 3) sawLo = true;
    if (v === 7) sawHi = true;
  }
  ok(sawLo && sawHi);
});

test('shuffle is a permutation and is seed stable', function () {
  const base = [1, 2, 3, 4, 5, 6, 7, 8];
  const a = new Rng(4).shuffle(base.slice());
  const b = new Rng(4).shuffle(base.slice());
  deepEq(a, b);
  deepEq(a.slice().sort(function (x, y) { return x - y; }), base);
});

test('chance(0) never fires and chance(1) always does', function () {
  const r = new Rng(5);
  for (let i = 0; i < 500; i++) { ok(!r.chance(0)); ok(r.chance(1)); }
});

test('jitter stays at or above one and centres on the base', function () {
  const r = new Rng(6);
  let sum = 0;
  for (let i = 0; i < 4000; i++) {
    const v = r.jitter(20, 10);
    ok(v >= 1);
    sum += v;
  }
  const mean = sum / 4000;
  ok(Math.abs(mean - 20) < 1, 'mean was ' + mean);
});
