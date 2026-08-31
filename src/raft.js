'use strict';

// A single Raft server, written as a pure state machine. It never touches the
// clock, the network or the disk itself: the caller feeds it a virtual time,
// hands it messages, and drains whatever it wants to send. That is the only
// reason the simulator in sim.js can replay a run byte for byte.
//
// Sections roughly follow figure 2 of the Raft paper (Ongaro and Ousterhout).

const FOLLOWER = 'follower';
const CANDIDATE = 'candidate';
const LEADER = 'leader';

const DEFAULTS = {
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 50,
  maxEntriesPerAppend: 64,
};

class RaftNode {
  constructor(id, peers, opts) {
    const o = Object.assign({}, DEFAULTS, opts || {});
    this.id = id;
    this.peers = peers.filter(function (p) { return p !== id; });
    this.quorum = Math.floor((this.peers.length + 1) / 2) + 1;
    this.opts = o;
    this.rng = o.rng; // used only to jitter election timeouts

    // Persistent state. Anything in here has to survive a crash.
    this.currentTerm = 0;
    this.votedFor = null;
    // Index 0 is a sentinel so that prevLogIndex 0 always resolves. Real entries
    // start at index 1, which keeps the arithmetic identical to the paper.
    this.log = [{ index: 0, term: 0, cmd: null }];

    // Volatile state, wiped on crash.
    this.state = FOLLOWER;
    this.commitIndex = 0;
    this.lastApplied = 0;
    this.leaderId = null;
    this.votesGranted = new Set();
    this.nextIndex = new Map();
    this.matchIndex = new Map();

    this.outbox = [];
    this.appliedOut = [];
    this.persistOps = 0; // how many times persistent state changed, for the stats
    this.electionDeadline = 0;
    this.heartbeatDeadline = 0;
    this.resetElectionTimer(0);
  }

  // ---- small helpers -------------------------------------------------------

  lastIndex() {
    return this.log[this.log.length - 1].index;
  }

  lastTerm() {
    return this.log[this.log.length - 1].term;
  }

  // The log is dense and starts at index 0, so the offset is the index itself.
  // Kept as a method anyway because snapshotting would change it.
  entryAt(index) {
    if (index < 0 || index >= this.log.length) return null;
    return this.log[index];
  }

  termAt(index) {
    const e = this.entryAt(index);
    return e === null ? -1 : e.term;
  }

  markPersisted() {
    this.persistOps++;
  }

  resetElectionTimer(now) {
    const o = this.opts;
    const span = o.electionTimeoutMax - o.electionTimeoutMin;
    const extra = this.rng ? this.rng.int(span + 1) : Math.floor(span / 2);
    this.electionDeadline = now + o.electionTimeoutMin + extra;
  }

  send(msg) {
    this.outbox.push(msg);
  }

  drain() {
    const out = this.outbox;
    this.outbox = [];
    return out;
  }

  takeApplied() {
    const out = this.appliedOut;
    this.appliedOut = [];
    return out;
  }

  // ---- persistence ---------------------------------------------------------

  persistentState() {
    return {
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      log: this.log.map(function (e) { return { index: e.index, term: e.term, cmd: e.cmd }; }),
    };
  }

  restore(saved) {
    this.currentTerm = saved.currentTerm;
    this.votedFor = saved.votedFor;
    this.log = saved.log.map(function (e) { return { index: e.index, term: e.term, cmd: e.cmd }; });
    this.state = FOLLOWER;
    this.commitIndex = 0;
    this.lastApplied = 0;
    this.leaderId = null;
    this.votesGranted = new Set();
    this.nextIndex = new Map();
    this.matchIndex = new Map();
    this.outbox = [];
    this.appliedOut = [];
  }

  // ---- role transitions ----------------------------------------------------

  stepDown(term, now) {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.markPersisted();
    }
    if (this.state !== FOLLOWER) {
      this.state = FOLLOWER;
      this.votesGranted = new Set();
      this.nextIndex = new Map();
      this.matchIndex = new Map();
    }
    this.resetElectionTimer(now);
  }

  becomeCandidate(now) {
    this.state = CANDIDATE;
    this.currentTerm += 1;
    this.votedFor = this.id;
    this.markPersisted();
    this.leaderId = null;
    this.votesGranted = new Set([this.id]);
    this.resetElectionTimer(now);
    for (const p of this.peers) {
      this.send({
        type: 'RequestVote',
        from: this.id,
        to: p,
        term: this.currentTerm,
        lastLogIndex: this.lastIndex(),
        lastLogTerm: this.lastTerm(),
      });
    }
    // A single node cluster wins its own election immediately.
    if (this.votesGranted.size >= this.quorum) this.becomeLeader(now);
  }

  becomeLeader(now) {
    this.state = LEADER;
    this.leaderId = this.id;
    this.nextIndex = new Map();
    this.matchIndex = new Map();
    for (const p of this.peers) {
      this.nextIndex.set(p, this.lastIndex() + 1);
      this.matchIndex.set(p, 0);
    }
    this.heartbeatDeadline = now; // heartbeat right away to suppress other elections
  }

  // ---- driving the node ----------------------------------------------------

  tick(now) {
    if (this.state === LEADER) {
      if (now >= this.heartbeatDeadline) {
        this.heartbeatDeadline = now + this.opts.heartbeatInterval;
        for (const p of this.peers) this.sendAppend(p);
      }
    } else if (now >= this.electionDeadline) {
      this.becomeCandidate(now);
    }
    this.applyCommitted();
  }

  // Client entry point. Only a leader accepts a proposal; anything else hands
  // back the leader it last heard from so the client can redirect.
  propose(cmd, now) {
    if (this.state !== LEADER) {
      return { ok: false, leaderHint: this.leaderId };
    }
    const entry = { index: this.lastIndex() + 1, term: this.currentTerm, cmd: cmd };
    this.log.push(entry);
    this.markPersisted();
    this.matchIndex.set(this.id, entry.index);
    for (const p of this.peers) this.sendAppend(p);
    this.advanceCommitIndex();
    return { ok: true, index: entry.index, term: entry.term };
  }

  sendAppend(peer) {
    const next = this.nextIndex.get(peer);
    const prevLogIndex = next - 1;
    const prevLogTerm = this.termAt(prevLogIndex);
    const entries = this.log
      .slice(next, next + this.opts.maxEntriesPerAppend)
      .map(function (e) { return { index: e.index, term: e.term, cmd: e.cmd }; });
    this.send({
      type: 'AppendEntries',
      from: this.id,
      to: peer,
      term: this.currentTerm,
      prevLogIndex: prevLogIndex,
      prevLogTerm: prevLogTerm,
      entries: entries,
      leaderCommit: this.commitIndex,
    });
  }

  handle(msg, now) {
    // Rule for all servers: a larger term always wins and demotes us first.
    if (msg.term > this.currentTerm) this.stepDown(msg.term, now);

    switch (msg.type) {
      case 'RequestVote': this.onRequestVote(msg, now); break;
      case 'RequestVoteResp': this.onRequestVoteResp(msg, now); break;
      case 'AppendEntries': this.onAppendEntries(msg, now); break;
      case 'AppendEntriesResp': this.onAppendEntriesResp(msg, now); break;
      default: throw new Error('unknown message type ' + msg.type);
    }
    this.applyCommitted();
  }

  onRequestVote(msg, now) {
    let granted = false;
    if (msg.term === this.currentTerm) {
      const free = this.votedFor === null || this.votedFor === msg.from;
      // "At least as up to date" compares the last term first, then the length.
      const upToDate =
        msg.lastLogTerm > this.lastTerm() ||
        (msg.lastLogTerm === this.lastTerm() && msg.lastLogIndex >= this.lastIndex());
      if (free && upToDate) {
        granted = true;
        if (this.votedFor !== msg.from) {
          this.votedFor = msg.from;
          this.markPersisted();
        }
        // Only reset the timer when actually granting, otherwise a node that
        // keeps rejecting votes would never start its own election.
        this.resetElectionTimer(now);
      }
    }
    this.send({
      type: 'RequestVoteResp',
      from: this.id,
      to: msg.from,
      term: this.currentTerm,
      granted: granted,
    });
  }

  onRequestVoteResp(msg, now) {
    if (this.state !== CANDIDATE) return;
    if (msg.term !== this.currentTerm) return; // stale reply from an older election
    if (!msg.granted) return;
    this.votesGranted.add(msg.from);
    if (this.votesGranted.size >= this.quorum) this.becomeLeader(now);
  }

  onAppendEntries(msg, now) {
    if (msg.term < this.currentTerm) {
      this.send({
        type: 'AppendEntriesResp', from: this.id, to: msg.from,
        term: this.currentTerm, success: false, matchIndex: 0,
        conflictIndex: 0, conflictTerm: -1,
      });
      return;
    }

    // Valid leader for this term. A candidate that hears from it gives up.
    if (this.state !== FOLLOWER) this.state = FOLLOWER;
    this.leaderId = msg.from;
    this.resetElectionTimer(now);

    // Consistency check on prevLogIndex/prevLogTerm.
    if (msg.prevLogIndex > this.lastIndex() || this.termAt(msg.prevLogIndex) !== msg.prevLogTerm) {
      this.send({
        type: 'AppendEntriesResp', from: this.id, to: msg.from,
        term: this.currentTerm, success: false, matchIndex: 0,
        conflictIndex: 0, conflictTerm: -1,
      });
      return;
    }

    // Append, truncating only where an entry actually conflicts. Truncating
    // blindly would throw away entries that a delayed duplicate already covers.
    let changed = false;
    for (let i = 0; i < msg.entries.length; i++) {
      const e = msg.entries[i];
      const existing = this.entryAt(e.index);
      if (existing === null) {
        this.log.push({ index: e.index, term: e.term, cmd: e.cmd });
        changed = true;
      } else if (existing.term !== e.term) {
        this.log.length = e.index;
        this.log.push({ index: e.index, term: e.term, cmd: e.cmd });
        changed = true;
      }
    }
    if (changed) this.markPersisted();

    const lastNew = msg.entries.length > 0
      ? msg.entries[msg.entries.length - 1].index
      : msg.prevLogIndex;
    // The paper writes this as min(leaderCommit, index of last new entry). Taken
    // literally that lets commitIndex move backwards: a delayed AppendEntries
    // carrying a low prevLogIndex and no entries has a small "last new entry"
    // even though leaderCommit is large. Clamping against the current value is
    // the missing half of the rule.
    if (msg.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.max(this.commitIndex, Math.min(msg.leaderCommit, lastNew));
    }

    this.send({
      type: 'AppendEntriesResp', from: this.id, to: msg.from,
      term: this.currentTerm, success: true, matchIndex: lastNew,
      conflictIndex: 0, conflictTerm: -1,
    });
  }

  onAppendEntriesResp(msg, now) {
    if (this.state !== LEADER) return;
    if (msg.term !== this.currentTerm) return;
    if (msg.success) {
      if (msg.matchIndex > (this.matchIndex.get(msg.from) || 0)) {
        this.matchIndex.set(msg.from, msg.matchIndex);
      }
      this.nextIndex.set(msg.from, this.matchIndex.get(msg.from) + 1);
      this.advanceCommitIndex();
      // If the follower is still behind, keep streaming instead of waiting for
      // the next heartbeat.
      if (this.matchIndex.get(msg.from) < this.lastIndex()) this.sendAppend(msg.from);
    } else {
      const next = this.nextIndex.get(msg.from);
      if (next > 1) this.nextIndex.set(msg.from, next - 1);
      this.sendAppend(msg.from);
    }
  }

  // Figure 8 of the paper: a leader may only commit by counting replicas for an
  // entry from its own term. Older entries then commit indirectly.
  advanceCommitIndex() {
    const marks = [this.lastIndex()];
    for (const p of this.peers) marks.push(this.matchIndex.get(p) || 0);
    marks.sort(function (a, b) { return b - a; });
    const candidate = marks[this.quorum - 1];
    if (candidate > this.commitIndex && this.termAt(candidate) === this.currentTerm) {
      this.commitIndex = candidate;
    }
  }

  applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const e = this.entryAt(this.lastApplied);
      this.appliedOut.push({ index: e.index, term: e.term, cmd: e.cmd });
    }
  }
}

module.exports = { RaftNode, FOLLOWER, CANDIDATE, LEADER, DEFAULTS };
