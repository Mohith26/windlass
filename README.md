# Windlass

A Raft implementation that I test by running it inside a deterministic simulator
instead of on a real network.

The consensus code is the smaller half of this repo. The larger half is the
machinery that tries to break it: a seeded discrete event simulator, a fault
schedule that goes after whoever is currently leader, a continuous checker for
the five safety properties from the Raft paper, and a linearizability checker
that reads the client history afterwards and asks whether the whole run could
have come from a single register handling one operation at a time.

No dependencies. Node 18 or newer.

```
node tests/run.js     # 117 tests
node bench/run.js     # writes results/*.json
```

## Why a simulator

I started this because I wanted to know whether my Raft was correct, and I could
not think of an honest way to answer that by running five processes and killing
them by hand. Bugs in consensus code live in orderings you will not reproduce
twice, so a test that fails once and then passes forever is worse than useless.

So nothing here touches the clock, the network or the disk directly. A node is a
state machine you feed a virtual timestamp and a message, and drain messages out
of. Everything random comes from one seeded PRNG. Two runs of the same seed
produce byte identical statistics, which means a failure is a five digit number I
can put in a test rather than a story about something I saw once.

That turned out to matter almost immediately. The first real bug below was found
by seed 1.

## The pieces

| File | What it does |
| --- | --- |
| `src/raft.js` | The consensus algorithm. Pure state machine, no IO |
| `src/kv.js` | The replicated state machine, plus the client session table |
| `src/sim.js` | Virtual clock, event queue, latency, loss, duplication, partitions, crashes |
| `src/scenario.js` | Generates a fault schedule from a seed and runs one whole experiment |
| `src/client.js` | Simulated clients that retry, redirect, time out, and record a history |
| `src/invariants.js` | The five safety properties, checked after every node step |
| `src/linearizability.js` | Offline check of the client history |
| `src/mutants.js` | Deliberately broken variants used as negative controls |
| `src/scripted.js` | Two hand built histories that random search never reproduced |

## What is actually implemented

Leader election with randomised timeouts, log replication with the prevLogIndex
consistency check and conflict driven backtracking, the up to date log rule on
votes, the figure 8 restriction that a leader may only commit by counting
replicas of an entry from its own term, persistence of term, vote and log across
crashes, and client sessions so that a retried request cannot take effect twice.

Not implemented: log compaction and snapshots, membership changes, pre vote,
leader leases or any lease based read path, and any real transport. Reads go
through the log like writes, which is the slow but obviously correct option.

## Faults the simulator can inject

Per link latency drawn from a range, message loss, message duplication, arbitrary
partitions checked at both send and delivery time, and crashes that wipe every
piece of volatile state including the state machine, so a restarted node has to
rebuild itself by replaying its log.

Random partitions turned out to be a weak fuzzer. Most of the interesting
behaviour is in the moment a leader loses contact with a majority while some of
its entries are already out there, so the schedule now spends most of its weight
on isolating the current leader, stranding it with a minority, or crashing it
outright. It also has quiet windows where clients stop sending, because a leader
holding uncommitted entries from an older term with no new traffic is the only
situation where the figure 8 rule is doing any work at all.

## What the fuzzer found

**commitIndex could move backwards on a follower.** Figure 2 of the paper writes
the follower rule as `commitIndex = min(leaderCommit, index of last new entry)`.
Implemented literally, a delayed AppendEntries carrying a low prevLogIndex and no
entries has a very small "index of last new entry" while still carrying a large
leaderCommit, so commitIndex jumps down. Seed 1 hit it in under three hundred
milliseconds of virtual time. Clamping against the current value fixes it.

Nothing was lost in practice, because `lastApplied` only ever moves forward and
so nothing was reapplied, but it is a real invariant violation and it is the kind
of latent problem that turns into a live one the moment anything else reads
commitIndex. It is now pinned by a unit test as well as by the fuzzer.

**One of the failures was my checker, not my Raft.** Sixty of the first hundred
and fifty seeds reported a Leader Append Only violation. All sixty were wrong. I
was recording that a node had been leader and then flagging any later truncation
of its log, but a node that has since stepped down is supposed to have its tail
overwritten by whoever won the next election. The rule only says anything inside
a single leadership epoch, so the check now compares the term as well as the
role. Worth writing down because a false positive at that rate would have buried
a real failure.

## Negative controls

Green tests only tell you that nothing tripped. To find out whether the tripwires
work, `src/mutants.js` holds six variants that each remove exactly one rule from
the algorithm, and the harness has to catch all six.

| Mutant | Caught by | Detection rate | First failing seed |
| --- | --- | --- | --- |
| `small-quorum` | fuzz | 100 percent | 1 |
| `vote-without-log-check` | fuzz | 90 percent | 1 |
| `blind-truncate` | fuzz | 35 percent | 4 |
| `accept-stale-append` | fuzz | 5 percent | 16 |
| `commit-across-terms` | scripted | 0 percent by fuzzing | never |
| `forget-vote-on-crash` | scripted | 0 percent by fuzzing | never |

The last two survived every random sweep I threw at them, which is the honest and
slightly annoying result. Both need a very specific ordering of elections and
crashes, so they are scripted by hand in `src/scripted.js`:

- **figure 8** replicates an old term entry onto a majority through a later
  leader, then lets a node holding a higher term entry at the same index win the
  next election. Against the real implementation the entry stays uncommitted and
  nothing fires. With the term check removed, commitIndex reaches 1 and the run
  produces leader completeness, committed stability and state machine safety
  violations.
- **duplicate vote** has a node vote, crash, and come back having forgotten the
  vote. It votes a second time in the same term and two different candidates both
  reach a majority, which the election safety check catches.

The same forty seeds run against the unmutated implementation stay clean, so the
mutant results are not just noise.

## Numbers

From `results/`, all measured on Apple Silicon arm64 under Node 20. Latencies are
in virtual milliseconds, which is the only unit that means anything here.

600 seeded runs across six fault profiles: **0 safety violations, 0 convergence
failures, 0 non linearizable histories**, over 287,723 completed client
operations and 348,845 committed log entries.

| Profile | Nodes | Failures | Client ops | Committed | Latency p50 / p99 |
| --- | --- | --- | --- | --- | --- |
| clean network | 5 | 0 | 93,686 | 93,850 | 31 / 47 |
| lossy network, 15 percent loss | 5 | 0 | 78,097 | 88,427 | 41 / 63 |
| leader targeted faults | 5 | 0 | 35,572 | 47,672 | 36 / 1,652 |
| leader targeted faults | 3 | 0 | 36,502 | 50,431 | 33 / 1,643 |
| leader targeted faults | 7 | 0 | 35,407 | 46,453 | 38 / 1,631 |
| 30 percent loss, faults every 200ms | 5 | 0 | 8,459 | 22,012 | 80 / 2,069 |

Time to elect a new leader after the current one is killed, with an election
timeout window of 150 to 300 virtual ms:

| Cluster | p50 | p99 | max |
| --- | --- | --- | --- |
| 3 nodes | 222 | 572 | 572 |
| 5 nodes | 184 | 522 | 522 |
| 7 nodes | 179 | 504 | 504 |

The simulator itself runs about 150,000 steps per second and gets through roughly
9,100 committed entries per second of wall clock, including the checker.

## The checker used to be the bottleneck

The invariant checker runs after every node step, and my first version rescanned
every log from index zero every time. That is quadratic in the log length and it
made the checker roughly a hundred and seventy times slower than the thing it was
watching:

| Virtual ms | Committed entries | No checker | Rescan | Incremental |
| --- | --- | --- | --- | --- |
| 1,500 | 75 | 6ms | 232ms | 8ms |
| 3,000 | 173 | 6ms | 1,227ms | 14ms |
| 4,500 | 276 | 8ms | 3,092ms | 25ms |
| 6,000 | 374 | 11ms | 5,632ms | 33ms |

The fix is to track a shadow copy of each log and only look at what changed. A
truncation is always followed by appends from a higher term, so a change in
either the length or the last term is enough to trigger the slow path, and
everything else is a pure append. Both modes are still in the code behind
`new InvariantChecker({ incremental: false })`, because I wanted this table to be
a measurement rather than a number I remembered.

## The linearizability checker

Every key behaves as an independent register, and linearizability composes over
independent objects, so the history is split per key and each piece is checked on
its own. Within a piece the search is the usual one: an operation can go next
only if no other remaining operation already returned before it was invoked.

The interesting part is operations that never returned, which happens whenever a
client times out. Those are genuinely unknown, so the checker is allowed to place
them anywhere after their invocation or to leave them out entirely, and it only
validates return values for operations that actually came back. Drops commute, so
offering the drop branch on the earliest pending operation alone is still
complete and keeps the fan out small.

The search is bounded. If it runs out of budget it reports `unknown` rather than
`ok`, because a checker that guesses is worse than no checker.

## Tests

117 tests, 74,529 assertions, about 3 seconds.

```
tests/rand.test.js             9 tests   determinism, distribution, seed spread
tests/kv.test.js              13 tests   command semantics and the session table
tests/raft.test.js            43 tests   elections, voting, log repair, commit rules
tests/sim.test.js             17 tests   replay determinism, partitions, crash and restart
tests/linearizability.test.js 16 tests   known good and known bad histories
tests/scripted.test.js         6 tests   figure 8 and duplicate vote, with and without mutants
tests/fuzz.test.js            13 tests   short sweeps plus mutation coverage
```

The linearizability tests matter more than their count suggests, since a checker
that says yes to everything would make the whole fuzzing story worthless. Half of
them are histories that must be rejected.

## Limitations

- Virtual time is not real time. Nothing here says anything about syscall
  overhead, TCP behaviour or fsync latency.
- Throughput numbers describe the simulator, not a server.
- No snapshots, so a long run keeps the whole log in memory and a restarted node
  replays all of it.
- Reads go through the log. Lease based reads would be faster and would need a
  different safety argument.
- The linearizability search is exponential in the worst case. It is fine at the
  concurrency levels here, three to five clients, and it says so when it is not.
