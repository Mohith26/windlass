'use strict';

const { LEADER } = require('./raft');

// The five safety properties from the Raft paper, checked continuously while a
// simulation runs. A fuzzer is only as good as the assertions it can trip, so
// this file is where most of the real value of the project lives.
//
//   1. Election Safety      at most one leader per term
//   2. Leader Append-Only   a leader never rewrites or drops its own entries
//   3. Log Matching         same index and term implies the same command, and
//                           implies identical logs up to that point
//   4. Leader Completeness  a committed entry survives in every future leader
//   5. State Machine Safety no two nodes apply different commands at an index
//
// Everything here runs after every single node step, so all of it is written to
// touch only what changed since the previous call. The first version rescanned
// each log from index 0 on every check and that alone was three hundred times
// slower than the simulation it was watching.

function cmdKey(cmd) {
  return cmd === null || cmd === undefined ? 'null' : JSON.stringify(cmd);
}

class InvariantChecker {
  // incremental defaults to true. Setting it to false restores the original
  // rescan-everything behaviour, which is kept only so the benchmark can measure
  // the difference honestly instead of quoting a number from memory.
  constructor(opts) {
    this.incremental = !(opts && opts.incremental === false);
    this.violations = [];
    this.leaderByTerm = new Map();
    this.entryByIndexTerm = new Map();
    this.appliedByIndex = new Map();
    this.committed = new Map();
    this.commitWatermark = new Map();
    this.shadow = new Map();
    this.commitScan = new Map();
    this.applyCursor = 0;
    this.checks = 0;
    this.rescans = 0; // how often a truncation forced the slow path
  }

  fail(rule, detail) {
    this.violations.push({ rule: rule, detail: detail });
  }

  attach(cluster) {
    const self = this;
    cluster.observe(function (c) { self.check(c); });
    return this;
  }

  check(cluster) {
    this.checks++;
    this.checkLogs(cluster);
    this.checkLeaders(cluster);
    this.checkCommitted(cluster);
    this.checkApplied(cluster);
  }

  checkLogs(cluster) {
    for (const id of cluster.ids) {
      if (cluster.down.has(id)) continue;
      const node = cluster.nodes.get(id);
      const len = node.log.length;
      const lastTerm = node.log[len - 1].term;
      let prev = this.shadow.get(id);

      if (prev !== undefined && prev.node === node && prev.len === len && prev.lastTerm === lastTerm) continue;

      if (prev === undefined || prev.node !== node) {
        // First sight of this incarnation, for example straight after a restart.
        prev = { node: node, terms: [], len: 0, lastTerm: -1, wasLeader: false, term: -1 };
        this.shadow.set(id, prev);
      }

      // Fast path: the log only grew and the entry at the old tail is unchanged,
      // which is what an ordinary append looks like. Anything else means a
      // truncation happened and the whole prefix has to be re-examined.
      let diverge;
      const prevLen = prev.terms.length;
      if (!this.incremental) {
        diverge = 0;
      } else if (len >= prevLen && prevLen > 0 && node.log[prevLen - 1].term === prev.terms[prevLen - 1]) {
        diverge = prevLen;
      } else if (prevLen === 0) {
        diverge = 0;
      } else {
        this.rescans++;
        diverge = 0;
        const n = Math.min(len, prevLen);
        while (diverge < n && node.log[diverge].term === prev.terms[diverge]) diverge++;
      }

      // Only a truncation inside a single leadership epoch breaks this rule. A
      // node that has since stepped down is allowed, and expected, to have its
      // tail overwritten by whoever won the next election, so the term has to
      // match as well as the role. Getting this wrong was my first false
      // positive: sixty of the first hundred and fifty seeds "failed" on
      // truncations that happened long after the node stopped being leader.
      if (prev.wasLeader && node.state === LEADER && node.currentTerm === prev.term && diverge < prevLen) {
        this.fail('leader-append-only',
          id + ' rewrote index ' + diverge + ' while leader in term ' + node.currentTerm +
          ' (term ' + prev.terms[diverge] + ' -> ' + (diverge < len ? node.log[diverge].term : 'missing') + ')');
      }

      for (let i = Math.max(diverge, 1); i < len; i++) {
        const e = node.log[i];
        const key = e.index + ':' + e.term;
        const val = cmdKey(e.cmd);
        const seen = this.entryByIndexTerm.get(key);
        if (seen === undefined) this.entryByIndexTerm.set(key, val);
        else if (seen !== val) {
          this.fail('log-matching',
            'index ' + e.index + ' term ' + e.term + ' holds ' + seen + ' elsewhere but ' + val + ' on ' + id);
        }
      }

      prev.terms.length = Math.min(prev.terms.length, diverge);
      for (let i = prev.terms.length; i < len; i++) prev.terms.push(node.log[i].term);
      prev.len = len;
      prev.lastTerm = lastTerm;
      prev.wasLeader = node.state === LEADER;
      prev.term = node.currentTerm;
    }
  }

  checkLeaders(cluster) {
    for (const id of cluster.ids) {
      if (cluster.down.has(id)) continue;
      const node = cluster.nodes.get(id);
      if (node.state !== LEADER) continue;

      const held = this.leaderByTerm.get(node.currentTerm);
      if (held === undefined) {
        this.leaderByTerm.set(node.currentTerm, id);
        // Leader Completeness only says something at the moment of election, so
        // this runs once per term rather than on every check.
        for (const [index, rec] of this.committed) {
          const own = index < node.log.length ? node.log[index] : null;
          if (own === null || own.term !== rec.term) {
            this.fail('leader-completeness',
              'new leader ' + id + ' at term ' + node.currentTerm + ' is missing committed index ' +
              index + ' (term ' + rec.term + ')');
          }
        }
      } else if (held !== id) {
        this.fail('election-safety',
          'term ' + node.currentTerm + ' has two leaders: ' + held + ' and ' + id);
      }
    }
  }

  checkCommitted(cluster) {
    for (const id of cluster.ids) {
      if (cluster.down.has(id)) continue;
      const node = cluster.nodes.get(id);

      const mark = this.commitWatermark.get(id);
      if (mark !== undefined && mark.node === node && node.commitIndex < mark.value) {
        this.fail('commit-monotonic',
          id + ' commitIndex went from ' + mark.value + ' back to ' + node.commitIndex);
      }
      if (mark === undefined || mark.node !== node) this.commitWatermark.set(id, { node: node, value: node.commitIndex });
      else mark.value = node.commitIndex;

      let scan = this.commitScan.get(id);
      if (scan === undefined || scan.node !== node || !this.incremental) {
        scan = { node: node, upTo: 0 };
        this.commitScan.set(id, scan);
      }
      const top = Math.min(node.commitIndex, node.log.length - 1);
      for (let i = scan.upTo + 1; i <= top; i++) {
        const e = node.log[i];
        const rec = this.committed.get(i);
        if (rec === undefined) {
          this.committed.set(i, { term: e.term, cmd: cmdKey(e.cmd) });
        } else if (rec.term !== e.term || rec.cmd !== cmdKey(e.cmd)) {
          this.fail('committed-stability',
            'committed index ' + i + ' was term ' + rec.term + ' ' + rec.cmd +
            ' but ' + id + ' now has term ' + e.term + ' ' + cmdKey(e.cmd));
        }
      }
      if (top > scan.upTo) scan.upTo = top;
    }
  }

  checkApplied(cluster) {
    for (; this.applyCursor < cluster.applyLog.length; this.applyCursor++) {
      const a = cluster.applyLog[this.applyCursor];
      const key = cmdKey(a.cmd);
      const seen = this.appliedByIndex.get(a.index);
      if (seen === undefined) this.appliedByIndex.set(a.index, key);
      else if (seen !== key) {
        this.fail('state-machine-safety',
          'index ' + a.index + ' applied as ' + seen + ' but ' + a.id + ' applied ' + key);
      }
    }
  }

  // A deeper pass worth paying for once at the end of a run: full pairwise log
  // comparison, which covers the prefix half of Log Matching that the
  // incremental check skips, plus one last look at every committed index.
  finalCheck(cluster) {
    this.check(cluster);
    for (const id of cluster.ids) {
      if (cluster.down.has(id)) continue;
      const node = cluster.nodes.get(id);
      const top = Math.min(node.commitIndex, node.log.length - 1);
      for (let i = 1; i <= top; i++) {
        const e = node.log[i];
        const rec = this.committed.get(i);
        if (rec !== undefined && (rec.term !== e.term || rec.cmd !== cmdKey(e.cmd))) {
          this.fail('committed-stability',
            'committed index ' + i + ' was term ' + rec.term + ' but ' + id + ' ends with term ' + e.term);
        }
      }
    }

    const live = cluster.ids.filter(function (id) { return !cluster.down.has(id); });
    for (let a = 0; a < live.length; a++) {
      for (let b = a + 1; b < live.length; b++) {
        const la = cluster.nodes.get(live[a]).log;
        const lb = cluster.nodes.get(live[b]).log;
        const n = Math.min(la.length, lb.length);
        for (let i = n - 1; i >= 0; i--) {
          if (la[i].term === lb[i].term) {
            for (let j = 0; j < i; j++) {
              if (la[j].term !== lb[j].term || cmdKey(la[j].cmd) !== cmdKey(lb[j].cmd)) {
                this.fail('log-matching-prefix',
                  live[a] + ' and ' + live[b] + ' agree at index ' + i + ' but differ at index ' + j);
                break;
              }
            }
            break;
          }
        }
      }
    }
    return this.violations;
  }

  report() {
    return {
      ok: this.violations.length === 0,
      checks: this.checks,
      rescans: this.rescans,
      violations: this.violations.slice(0, 20),
      violationCount: this.violations.length,
    };
  }
}

module.exports = { InvariantChecker };
