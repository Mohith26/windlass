'use strict';

// A linearizability checker for the client histories the simulator produces.
//
// Every key in the store behaves as an independent register, and linearizability
// is compositional over independent objects, so I split the history per key and
// check each piece on its own. That turns one intractable search into a handful
// of small ones.
//
// The search itself is the usual one: an operation may go next only if no other
// remaining operation already returned before it was invoked. Operations that
// never returned (the client timed out) may be placed anywhere or left out
// entirely, which is exactly the freedom a real client has when it does not know
// whether its request landed.

const UNKNOWN = Symbol('unknown-result');

function applyOp(state, op) {
  switch (op.op) {
    case 'put': return { state: op.value, result: op.value };
    case 'get': return { state: state, result: state };
    case 'del': return { state: null, result: state !== null };
    case 'cas':
      if (state === op.expect) return { state: op.value, result: true };
      return { state: state, result: false };
    default: throw new Error('checker does not model ' + op.op);
  }
}

function stateKey(s) {
  return s === null ? 'N' : typeof s + ':' + String(s);
}

// history entries: { type: 'invoke' | 'ok' | 'info', proc, id, op, result, time }
function toOperations(history) {
  const byId = new Map();
  let order = 0;
  for (const ev of history) {
    if (ev.type === 'invoke') {
      byId.set(ev.id, { id: ev.id, op: ev.op, inv: order++, ret: Infinity, result: UNKNOWN, proc: ev.proc });
    } else {
      const rec = byId.get(ev.id);
      if (!rec) throw new Error('response without an invoke: ' + ev.id);
      rec.ret = order++;
      if (ev.type === 'ok') rec.result = ev.result;
      // 'info' leaves the result unknown and the op pending on purpose.
      if (ev.type === 'info') rec.ret = Infinity;
    }
  }
  return Array.from(byId.values()).sort(function (a, b) { return a.inv - b.inv; });
}

function partitionByKey(history) {
  const parts = new Map();
  for (const ev of history) {
    const key = ev.op.key;
    if (!parts.has(key)) parts.set(key, []);
    parts.get(key).push(ev);
  }
  return parts;
}

// Returns { ok, reason, explored } for a single key. explored is capped so a
// pathological history reports 'unknown' instead of running forever, and the
// caller is expected to treat unknown as "needs a human", not as a pass.
function checkRegister(ops, opts) {
  const limit = (opts && opts.limit) || 400000;
  const memo = new Set();
  let explored = 0;
  let exhausted = false;

  const remaining = ops.slice();

  function search(rem, state) {
    if (rem.length === 0) return true;
    if (explored >= limit) { exhausted = true; return false; }
    explored++;

    const key = stateKey(state) + '|' + rem.map(function (o) { return o.id; }).join(',');
    if (memo.has(key)) return false;
    memo.add(key);

    let minRet = Infinity;
    for (const o of rem) if (o.ret < minRet) minRet = o.ret;

    for (let i = 0; i < rem.length; i++) {
      const o = rem[i];
      if (o.inv > minRet) continue; // something else already returned before this started
      const next = applyOp(state, o);
      if (o.result !== UNKNOWN && !sameResult(next.result, o.result)) continue;
      const rest = rem.slice(0, i).concat(rem.slice(i + 1));
      if (search(rest, next.state)) return true;
      if (exhausted) return false;
    }

    // A pending operation is allowed to have never taken effect at all.
    for (let i = 0; i < rem.length; i++) {
      const o = rem[i];
      if (o.ret !== Infinity) continue;
      const rest = rem.slice(0, i).concat(rem.slice(i + 1));
      if (search(rest, state)) return true;
      if (exhausted) return false;
    }
    return false;
  }

  const ok = search(remaining, null);
  if (!ok && exhausted) return { ok: false, unknown: true, explored: explored };
  return { ok: ok, unknown: false, explored: explored };
}

function sameResult(a, b) {
  if (a === null && b === null) return true;
  return a === b;
}

function check(history, opts) {
  const parts = partitionByKey(history);
  const perKey = {};
  let ok = true;
  let unknown = false;
  let explored = 0;
  for (const [key, evs] of parts) {
    const ops = toOperations(evs);
    const res = checkRegister(ops, opts);
    perKey[key] = { ok: res.ok, unknown: res.unknown, ops: ops.length, explored: res.explored };
    explored += res.explored;
    if (res.unknown) unknown = true;
    else if (!res.ok) ok = false;
  }
  return { ok: ok && !unknown, unknown: unknown, perKey: perKey, explored: explored, keys: parts.size };
}

module.exports = { check, checkRegister, toOperations, partitionByKey, applyOp, UNKNOWN };
