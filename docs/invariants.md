# The five properties, and how each one is actually checked

Notes to myself, mostly. The paper states these as properties of an execution;
turning each into something a program can watch after every step took more
thought than I expected, so this is the mapping I settled on.

Everything below lives in `src/invariants.js` and runs as an observer on the
cluster, so it fires after every message delivery and every timer tick.

## 1. Election Safety

*At most one leader can be elected in a given term.*

A map from term to leader id. The first node seen in the leader state for a term
claims it; any other node claiming the same term is a violation. Cheap, and it
catches quorum arithmetic mistakes immediately.

## 2. Leader Append Only

*A leader never overwrites or deletes entries in its own log.*

This one needs care. The naive reading, "a node that was leader must never
truncate", is wrong, and it was my first false positive: a node that has stepped
down is supposed to have its tail overwritten by the next leader. The property
only says something inside a single leadership epoch, so the check compares the
term as well as the role, and only fires when a node is still leader in the same
term it was leader in at the previous observation.

## 3. Log Matching

*If two logs contain an entry with the same index and term, then the logs are
identical in all entries up through that index.*

Split in two, for cost reasons.

The cheap half runs continuously: every entry is registered under `index:term`,
and two nodes holding different commands under the same key is a violation. That
is the second sentence of the property, restricted to the entry itself.

The expensive half, the prefix claim, runs once at the end of a run. For every
pair of live nodes it finds the highest index where the terms agree and then
verifies every earlier entry matches. That is quadratic in the number of nodes
and linear in the log, which is fine once and unaffordable per step.

## 4. Leader Completeness

*If an entry is committed in a term, it is present in the logs of all leaders in
higher terms.*

The checker keeps a map of every index it has ever seen committed anywhere, along
with the term and command that were there. The check itself only means anything
at the moment a node becomes leader, so it runs once per term, when a term first
produces a leader: every already committed index must be present in the new
leader's log with the same term.

Alongside it there is a stronger and simpler check I called committed stability:
once an index has been observed committed, no node may ever be seen with a
different entry at that index. That is really a corollary, but it catches the
same class of bug earlier and with a much better error message.

## 5. State Machine Safety

*If a server has applied an entry at a given index, no other server will ever
apply a different entry for the same index.*

The simulator keeps an append only log of every application by every node, so
this is a scan of new entries since the last check against a map from index to
command. It is the cheapest check of the five and the most damning when it fires,
because by then the divergence is visible to clients.

## Two extras that are not in the paper

**Commit monotonicity.** A node's commitIndex must never decrease within one
incarnation. It is allowed to reset on restart, since commitIndex is volatile, so
the check is keyed on the node object rather than the node id. This is the check
that caught the first real bug in the implementation.

**Convergence.** After the fault window closes and everything is healed and
restarted, every live node must hold the same log prefix up to the minimum commit
index, and nodes at the same applied index must have identical state machine
digests. Strictly this follows from the properties above, but it is the assertion
that most closely matches what I actually want from the system, and it fails in a
much more readable way.
