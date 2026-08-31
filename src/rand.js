'use strict';

// Everything in this project has to be reproducible from a seed, so I avoid
// Math.random entirely. sfc32 is small, fast and passes PractRand, which is
// far more than I need here. The seed is expanded through splitmix32 first so
// that seeds 1, 2, 3 do not start from nearly identical states.

function splitmix32(a) {
  return function () {
    a = (a + 0x9e3779b9) | 0;
    let t = a ^ (a >>> 16);
    t = Math.imul(t, 0x21f0aaad);
    t = t ^ (t >>> 15);
    t = Math.imul(t, 0x735a2d97);
    t = t ^ (t >>> 15);
    return t >>> 0;
  };
}

class Rng {
  constructor(seed) {
    const mix = splitmix32(seed >>> 0);
    this.a = mix();
    this.b = mix();
    this.c = mix();
    this.d = mix();
    // Discard a few outputs so the first value is not a direct function of the seed.
    for (let i = 0; i < 12; i++) this.next();
  }

  next() {
    const t = (this.a + this.b | 0) + this.d | 0;
    this.d = (this.d + 1) | 0;
    this.a = this.b ^ (this.b >>> 9);
    this.b = (this.c + (this.c << 3)) | 0;
    this.c = (this.c << 21) | (this.c >>> 11);
    this.c = (this.c + t) | 0;
    return t >>> 0;
  }

  float() {
    return this.next() / 4294967296;
  }

  // Uniform integer in [0, n).
  int(n) {
    return Math.floor(this.float() * n);
  }

  // Uniform integer in [lo, hi], inclusive on both ends.
  range(lo, hi) {
    return lo + this.int(hi - lo + 1);
  }

  pick(arr) {
    return arr[this.int(arr.length)];
  }

  chance(p) {
    return this.float() < p;
  }

  shuffle(arr) {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = this.int(i + 1);
      const tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
    return arr;
  }

  // Saves me writing the same clamp everywhere when jittering latencies.
  jitter(base, spread) {
    return Math.max(1, Math.round(base + (this.float() * 2 - 1) * spread));
  }
}

module.exports = { Rng, splitmix32 };
