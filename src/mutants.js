'use strict';

// Negative controls. Passing tests only tell me that nothing tripped; they do
// not tell me the tripwires work. Each mutant here removes exactly one rule from
// the algorithm, and the harness is expected to catch every single one. If a
// mutant survives a long sweep, the harness has a hole in it and the green runs
// above it mean less than they look like they do.

const MUTANTS = {
  // Raft grants a vote only to a candidate whose log is at least as up to date
  // as its own. Without that a stale candidate can win and then overwrite
  // entries that were already committed elsewhere.
  'vote-without-log-check': function (node) {
    node.onRequestVote = function (msg, now) {
      let granted = false;
      if (msg.term === this.currentTerm) {
        const free = this.votedFor === null || this.votedFor === msg.from;
        if (free) {
          granted = true;
          if (this.votedFor !== msg.from) { this.votedFor = msg.from; this.markPersisted(); }
          this.resetElectionTimer(now);
        }
      }
      this.send({ type: 'RequestVoteResp', from: this.id, to: msg.from, term: this.currentTerm, granted: granted });
    };
  },

  // Figure 8: counting replicas is not enough to commit an entry from an
  // earlier term. Dropping the term check is the classic way to lose a
  // committed entry.
  'commit-across-terms': function (node) {
    node.advanceCommitIndex = function () {
      const marks = [this.lastIndex()];
      for (const p of this.peers) marks.push(this.matchIndex.get(p) || 0);
      marks.sort(function (a, b) { return b - a; });
      const candidate = marks[this.quorum - 1];
      if (candidate > this.commitIndex) this.commitIndex = candidate;
    };
  },

  // votedFor has to reach disk before the vote is answered. If a crash loses
  // it, one node can vote twice in the same term.
  'forget-vote-on-crash': function (node) {
    const original = node.persistentState.bind(node);
    node.persistentState = function () {
      const s = original();
      s.votedFor = null;
      return s;
    };
  },

  // A follower may only truncate where an entry actually conflicts. Truncating
  // unconditionally throws away entries that a delayed or duplicated
  // AppendEntries has already covered.
  'blind-truncate': function (node) {
    const original = node.onAppendEntries.bind(node);
    node.onAppendEntries = function (msg, now) {
      if (msg.term >= this.currentTerm && msg.prevLogIndex <= this.lastIndex() &&
          this.termAt(msg.prevLogIndex) === msg.prevLogTerm && msg.entries.length > 0) {
        this.log.length = msg.prevLogIndex + 1;
      }
      return original(msg, now);
    };
  },

  // Ignoring the term on an incoming AppendEntries lets a deposed leader keep
  // pushing its log onto followers.
  'accept-stale-append': function (node) {
    const original = node.onAppendEntries.bind(node);
    node.onAppendEntries = function (msg, now) {
      const patched = Object.assign({}, msg, { term: Math.max(msg.term, this.currentTerm) });
      return original(patched, now);
    };
  },

  // Half the cluster is not a majority. With an even split both halves can
  // elect a leader in the same term.
  'small-quorum': function (node) {
    node.quorum = Math.max(2, Math.floor((node.peers.length + 1) / 2));
  },
};

function list() {
  return Object.keys(MUTANTS);
}

function get(name) {
  const m = MUTANTS[name];
  if (!m) throw new Error('no mutant named ' + name);
  return m;
}

module.exports = { MUTANTS, list, get };
