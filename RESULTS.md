# Results

My own notes on every number that ends up in the README, plus how to regenerate
them. Machine was Apple Silicon arm64, Node 20, single process, nothing else
running. Every figure below comes out of a file in `results/`, and if a number is
not in one of those files it should not be in the README either.

```
node tests/run.js          # test suite
node bench/checker.js      # results/checker-overhead.json
node bench/latency.js      # results/latency.json
node bench/mutation.js     # results/mutation.json
node bench/sweep.js        # results/sweep.json
node bench/run.js          # all of the above plus results/summary.json
```

Each benchmark was run on its own rather than through `bench/run.js`, so
`results/summary.json` is stitched together from the four individual files. The
numbers are identical either way since everything is seeded.

## Test suite

```
windlass: 117/117 tests passed, 74529 assertions, 3117ms
```

The assertion count is dominated by the sweeps inside `tests/fuzz.test.js`, which
assert per seed. The unit files account for a few hundred.

## Sweep, results/sweep.json

100 seeds per profile, six profiles, 600 runs, 38.4 seconds.

- 0 safety violations
- 0 convergence failures
- 0 non linearizable histories
- 0 linearizability searches that ran out of budget
- 287,723 client operations completed
- 348,845 committed log entries
- 5,743,860 simulator steps, about 149,763 per second
- 22,472,253 messages sent

Per profile, latency in virtual ms:

| Profile | Nodes | Ops | Committed | p50 | p90 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| clean-network | 5 | 93,686 | 93,850 | 31 | 39 | 47 |
| lossy-network | 5 | 78,097 | 88,427 | 41 | 52 | 63 |
| faults-5 | 5 | 35,572 | 47,672 | 36 | 53 | 1,652 |
| faults-3 | 3 | 36,502 | 50,431 | 33 | 52 | 1,643 |
| faults-7 | 7 | 35,407 | 46,453 | 38 | 52 | 1,631 |
| brutal | 5 | 8,459 | 22,012 | 80 | 118 | 2,069 |

The p99 jump under fault injection is the expected shape. When a leader is killed
or partitioned away, any client waiting on it stalls until a new leader is elected
and the request is retried, so the tail sits near the election window rather than
near the round trip. The p50 barely moves, which is the point.

The brutal profile drops 30 percent of messages and injects a fault every 200
virtual ms. Throughput collapses to roughly a tenth of the clean case and the
cluster still never violates anything, which is the answer I wanted.

## Latency, results/latency.json

Time from killing the leader to a different node being leader and able to reach a
quorum, 60 seeds per cluster size, election timeout window 150 to 300 virtual ms,
one way link latency 5 to 20.

| Cluster | count | min | p50 | p90 | p99 | max | mean |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 nodes | 60 | 122 | 222 | 423 | 572 | 572 | 248.23 |
| 5 nodes | 60 | 126 | 184 | 241 | 522 | 522 | 194.9 |
| 7 nodes | 60 | 133 | 179 | 329 | 504 | 504 | 197.37 |

No run failed to elect within the 8 second window.

Larger clusters recovering slightly faster is not a mistake. With more nodes there
are more independent randomised timeouts, so the earliest one fires sooner, and
the extra vote traffic is cheap next to the timeout itself. The tail is set by
split votes, which is why all three sizes top out near two timeout periods.

Client latency end to end, 20 seeds each:

| Network | ops | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| quiet | 26,191 | 22 | 27 | 32 | 197 |
| 15 percent loss | 18,471 | 41 | 52 | 63 | 724 |

## Mutation coverage, results/mutation.json

40 seeds per mutant, plus 40 control runs on the unmutated implementation.

| Mutant | Detected by | Seeds detected | Rate | First seed | Invariants fired |
| --- | --- | --- | --- | --- | --- |
| vote-without-log-check | fuzz | 36 / 40 | 0.9 | 1 | committed stability, leader completeness, linearizability, state machine safety |
| commit-across-terms | scripted | 0 / 40 | 0 | none | committed stability, leader completeness, state machine safety |
| forget-vote-on-crash | scripted | 0 / 40 | 0 | none | election safety |
| blind-truncate | fuzz | 14 / 40 | 0.35 | 4 | committed stability, leader completeness, linearizability, state machine safety |
| accept-stale-append | fuzz | 2 / 40 | 0.05 | 16 | committed stability, leader completeness, linearizability, state machine safety |
| small-quorum | fuzz | 40 / 40 | 1.0 | 1 | election safety, log matching, committed stability, leader completeness, state machine safety, convergence, linearizability |

Control: 40 of 40 clean.

`accept-stale-append` at 5 percent is the weakest link in the whole harness and I
am not going to pretend otherwise. Two seeds in forty is enough to prove the
tripwire exists but not enough to trust it as regression coverage, which is why
the fuzz test for it walks up to thirty seeds before giving up.

## Checker overhead, results/checker-overhead.json

Same seed, same workload, three configurations.

| Virtual ms | Committed | Steps | No checker | Rescan | Incremental | Speedup | Overhead vs none |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1,500 | 75 | 1,097 | 6ms | 232ms | 8ms | 29.0x | 1.3x |
| 3,000 | 173 | 2,563 | 6ms | 1,227ms | 14ms | 87.6x | 2.3x |
| 4,500 | 276 | 4,039 | 8ms | 3,092ms | 25ms | 123.7x | 3.1x |
| 6,000 | 374 | 5,488 | 11ms | 5,632ms | 33ms | 170.7x | 3.0x |

The rescan column grows quadratically and the incremental column grows linearly,
which is exactly the difference between rescanning a log per step and tracking
what changed. The speedup number is therefore a function of how long the run is,
not a fixed property, so quoting the largest one on its own would be misleading
and the whole table belongs in the README.

Truncation rescans, the slow path in the incremental checker, fired zero times
across all 49,098 checks in the longest run. That is not a surprise once I looked
at what the benchmark actually does: it runs a healthy cluster with a little loss
and no partitions, so the leader never changes and no follower ever has to be
repaired. It does mean this table measures the fast path only, and the slow path
is exercised by the fault sweeps rather than here.

## Things I checked that are not headline numbers

- Replay determinism. Two runs of seed 77 with loss and duplication enabled
  produce identical statistics, identical leaders and identical commit indices.
  This is asserted in `tests/sim.test.js` rather than being a one off.
- A restarted node rebuilds its state machine to the same digest it had before
  the crash, verified in `tests/sim.test.js`.
- The client session table suppresses retries. Without it a client that times out
  and retries can get the same logical operation into the log twice, and a
  compare and swap would then be applied twice with different answers.
- A leader stranded in a minority does not step down, because plain Raft has no
  lease or quorum check. It also cannot commit anything, and that is what the
  test asserts. Asserting the first thing instead was my own mistake and cost me
  a red test.
