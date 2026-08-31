'use strict';
const fs = require('fs');
const path = require('path');

function save(name, data) {
  const dir = path.join(__dirname, '..', 'results');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, name);
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n');
  return file;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}

function summarise(values) {
  const s = values.slice().sort(function (a, b) { return a - b; });
  let sum = 0;
  for (const v of s) sum += v;
  return {
    count: s.length,
    min: s.length ? s[0] : null,
    p50: percentile(s, 50),
    p90: percentile(s, 90),
    p99: percentile(s, 99),
    max: s.length ? s[s.length - 1] : null,
    mean: s.length ? Math.round((sum / s.length) * 100) / 100 : null,
  };
}

module.exports = { save, percentile, summarise };
