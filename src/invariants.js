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

function cmdKey(cmd) {
  return cmd === null || cmd === undefined ? 'null' : JSON.stringify(cmd);
}

class InvariantChecker {
  constructor() {
    this.violations = [];
    this.leaderByTerm = new Map();
    this.entryByIndexTerm = new Map();
    this.appliedByIndex = new Map();
    this.committed = new Map();
    this.commitWatermark = new Map();
    this.shadow = new Map();
    this.nodeRef = new Map();
    this.applyCursor = 0;
    this.checks = 0;
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

  // Walks each live node's log, but only when something plausibly changed. A
  // truncation is always followed by appends from a higher term, so a change in
  // either the length or the last term is enough to trigger the full rescan.
  checkLogs(cluster) {
    for (const id of cluster.ids) {
      if (cluster.down.has(id)) continue;
      const node = cluster.nodes.get(id);
      const prev = this.shadow.get(id);
      const len = node.log.length;
      const lastTerm = node.log[len - 1].term;
      if (prev && prev.len === len && prev.lastTerm === lastTerm) continue;

      // Leader Append-Only: while this object has been leader its log may only
      // grow, never change underneath.
      if (prev && prev.wasLeader && this.nodeRef.get(id) === node) {
        for (let i = 0; i < prev.terms.length; i++) {
          if (i >= len || node.log[i].term !== prev.terms[i]) {
            this.fail('leader-append-only',
              id + ' rewrote index ' + i + ' while leader (term ' + prev.terms[i] + ' -> ' +
              (i < len ? node.log[i].term : 'missing') + ')');
            break;
          }
        }
      }

      // Log Matching: register every entry under index:term and complain if two
      // nodes ever disagree about what lives there.
      for (let i = 1; i < len; i++) {
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

      this.shadow.set(id, {
        len: len,
        lastTerm: lastTerm,
        terms: node.log.map(function (e) { return e.term; }),
        wasLeader: node.state === LEADER,
      });
      this.nodeRef.set(id, node);
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
        // Leader Completeness is only meaningful at the moment of election, so
        // check it the first time this term produces a leader.
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

      // commitIndex must never move backwards within one incarnation of a node.
      const mark = this.commitWatermark.get(id);
      if (mark !== undefined && mark.node === node && node.commitIndex < mark.value) {
        this.fail('commit-monotonic',
          id + ' commitIndex went from ' + mark.value + ' back to ' + node.commitIndex);
      }
      this.commitWatermark.set(id, { node: node, value: node.commitIndex });

      for (let i = 1; i <= node.commitIndex && i < node.log.length; i++) {
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
  // comparison, plus the prefix half of Log Matching that the incremental check
  // does not cover.
  finalCheck(cluster) {
    this.check(cluster);
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
                  live[a] + ' and ' + live[b] + ' agree at index ' + i +
                  ' but differ at index ' + j);
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
      violations: this.violations.slice(0, 20),
      violationCount: this.violations.length,
    };
  }
}

module.exports = { InvariantChecker };
