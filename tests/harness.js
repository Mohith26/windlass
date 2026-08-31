'use strict';

// A three function test harness. I did not want a dependency for this and the
// only things I actually need are a name, an assertion counter and a non zero
// exit code when something breaks.

const registry = [];

function test(name, fn) {
  registry.push({ name: name, fn: fn });
}

let asserts = 0;

function ok(cond, msg) {
  asserts++;
  if (!cond) throw new Error('expected truthy: ' + (msg || ''));
}

function eq(actual, expected, msg) {
  asserts++;
  if (actual !== expected) {
    throw new Error('expected ' + JSON.stringify(expected) + ' but got ' + JSON.stringify(actual) + (msg ? ' :: ' + msg : ''));
  }
}

function deepEq(actual, expected, msg) {
  asserts++;
  const a = JSON.stringify(actual);
  const b = JSON.stringify(expected);
  if (a !== b) throw new Error('expected ' + b + ' but got ' + a + (msg ? ' :: ' + msg : ''));
}

function throws(fn, msg) {
  asserts++;
  let threw = false;
  try { fn(); } catch (e) { threw = true; }
  if (!threw) throw new Error('expected a throw: ' + (msg || ''));
}

function run(label) {
  let pass = 0;
  const failures = [];
  const started = Date.now();
  for (const t of registry) {
    try {
      t.fn();
      pass++;
    } catch (e) {
      failures.push({ name: t.name, error: e.message });
    }
  }
  const ms = Date.now() - started;
  console.log('');
  console.log(label + ': ' + pass + '/' + registry.length + ' tests passed, ' + asserts + ' assertions, ' + ms + 'ms');
  for (const f of failures) {
    console.log('  FAIL ' + f.name);
    console.log('       ' + f.error);
  }
  return { total: registry.length, pass: pass, asserts: asserts, failures: failures, ms: ms };
}

function reset() {
  registry.length = 0;
  asserts = 0;
}

module.exports = { test, ok, eq, deepEq, throws, run, reset };
