# Lattice

Lattice is a replicated key-value store built on the Raft consensus algorithm, written from scratch in C# (.NET 10) with no consensus or networking libraries beyond the standard library. It supports get, put, delete and compare-and-swap, with client sessions so retried commands apply exactly once, and linearizable reads through Raft's ReadIndex protocol. The same Raft code runs in two places: inside a deterministic simulator, where a whole cluster, its network, its disks and its faults run on one thread from a single seed, and as separate processes talking over TCP with a small command-line client.

Most of the work is in proving it correct. A chaos test runs thousands of seeds, each with its own random schedule of partitions, crashes, pauses, message loss, duplication and clock drift. Every run is checked continuously against Raft's safety properties, and every client history is checked for linearizability by a checker written from scratch (Wing and Gong search with Lowe's memoization, split per key). Five deliberate bugs can be switched on to show that these checks catch real mistakes, and the bug injection results below give the number of seeds each one took to catch.

## Features

- Raft as in the paper: randomized election timeouts, terms and votes with the log up-to-date check, log replication with the consistency check and conflict truncation, the current-term commit rule (with a no-op entry at the start of each term), and apply in log order.
- Term, vote and log are written to disk and fsynced before any reply that depends on them. On the TCP server one fsync covers a whole batch of requests (group commit).
- Log compaction with snapshots, and InstallSnapshot for followers that fall behind the compacted log.
- Leader-side replication with probe, pipelined replicate and snapshot states per follower, conflict hints that skip back a whole term, and an in-flight limit.
- Check-quorum: a leader that cannot reach a majority for an election timeout steps down.
- A key-value state machine with get, put, delete and compare-and-swap, and client sessions (client id plus sequence number) that make retries and network duplicates apply once.
- Linearizable reads with ReadIndex (no disk write per read), or optionally through the log.
- One `IRaftTransport` interface with two implementations: the simulator's network and a TCP transport. One `IDurableStore` interface with two implementations: a simulated disk that loses unsynced writes on a crash, and a file store with CRC-checked records, atomic replacement of small files and torn-tail recovery.
- A deterministic simulator with simulated clocks (per-node drift and jitter), network (delay, loss, duplication, reordering, partitions between servers), disks, crashes, restarts and pauses. A run is reproducible from its seed, and a failing run can be traced and shrunk (greedily, one fault at a time) to a fault list from which no single fault can be removed.
- A linearizability checker for key-value histories that handles operations with unknown outcomes (timeouts).
- A checker for Raft's safety properties (election safety, log matching, leader completeness, state machine safety) that runs inside the simulation at every election, append, truncation, commit and apply, so a violation is reported at the step that caused it.
- A CLI: `lattice serve`, `lattice cluster`, `put`, `get`, `delete`, `cas`, `status`, `chaos`, `bugs` and `bench`.

Membership changes are not implemented (see Limitations).

## Architecture

```
   client CLI / LatticeClient                      chaos runner (seeds, faults, shrinking)
          |  ClientRequest (client id, seq)                   |
          v                                                   v
   +--------------------------------------+       +------------------------------+
   | KvServer                             |       | Simulation (one thread)      |
   |   writes -> Propose                  |       |   event queue (time, seq)    |
   |   reads  -> ReadIndex (or the log)   |       |   SimNetwork  SimDisk        |
   +-------------------+------------------+       |   clients -> history         |
                       |                          |   RaftInvariants (observer)  |
   +-------------------v------------------+       +--------------+---------------+
   | RaftNode (single-threaded, no clock) |<--------------------+
   |   Tick / Receive / Propose / ReadIndex, then Flush:        |
   |   fsync the store, advance commit, apply, send messages    |
   +------+---------------------+--------------+---------------+
          |                     |              |
   IRaftTransport          IDurableStore   IStateMachine
   (TCP or simulated)      (file or SimDisk)  (KvStateMachine + sessions)

   After a run: LinearizabilityChecker(history), per key
```

```
src/Lattice.Core/Raft     RaftNode, RaftLog, messages, options, injected bugs
src/Lattice.Core/Kv       KvStateMachine (sessions), KvServer, commands
src/Lattice.Core/Storage  FileDurableStore
src/Lattice.Core/Net      TCP ServerHost, wire format, LatticeClient
src/Lattice.Core/Sim      Simulation, SimDisk, FaultPlan, RaftInvariants, ChaosRunner
src/Lattice.Core/Check    History and LinearizabilityChecker
src/Lattice.Cli           the lattice command (serve, cluster, client, chaos, bugs, bench)
tests/Lattice.Tests       xUnit tests
docs/HOW_IT_WORKS.md      design walkthrough, the injected bugs, interview Q&A
```

## Quick start

Requires the .NET 10 SDK. Build once:

```
dotnet build -c Release
```

The `lattice` executable is then at `src/Lattice.Cli/bin/Release/net10.0/lattice` (`lattice.exe` on Windows); the examples below call it `lattice`. `dotnet run --project src/Lattice.Cli -c Release -- <args>` works too.

Start a three-node cluster on ports 7001-7003 (each node is its own process, data in `data/n1..n3`):

```
lattice cluster --nodes 3
```

In another terminal:

```
lattice put color blue
lattice get color
lattice cas color blue green
lattice cas color --absent red      # only if the key does not exist
lattice delete color
lattice status
```

`status` prints each node's role, term, leader, commit index, applied index, log length and snapshot index. Kill the leader's process (`lattice cluster` prints the PIDs) and run `lattice get color` again: the client follows the new leader. Press Enter or Ctrl+C in the cluster terminal to stop every node; nodes also exit by themselves if the process that started them goes away.

To run the nodes by hand:

```
lattice serve --id 1 --cluster 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 --data data/n1
```

The `--cluster` list is in node-id order, and clients take the same list with `--nodes`.

## Tests, the chaos campaign and bug injection

```
dotnet test                                    # 107 xUnit tests, about 15-35 s on this machine (Debug)
lattice chaos --seeds 10000                    # the long campaign (2 threads by default; --parallel N)
lattice chaos --seed 42 --trace                # replay one seed and print what happened
lattice chaos --seed 42 --bug stale-read --shrink    # find a smaller fault list that still fails
lattice bugs --seeds 2000                      # the bug-injection table below
lattice bench                                  # the benchmarks below
```

The tests cover the Raft node message by message (votes, the up-to-date check, fsync before replies, conflict truncation, reordered appends, the commit rule, ReadIndex confirmation, step-down, check-quorum, snapshots and restarts), Figure 8 of the Raft paper replayed step by step on a hand-driven five-node cluster, the key-value state machine and sessions, the file store (restart, truncation, snapshot, torn tail, a crash between snapshot and log rewrite, corruption), the wire format, the linearizability checker (hand-made linearizable and non-linearizable histories, unknown outcomes, and agreement with a brute-force search on 3,000 random histories), simulator determinism, InstallSnapshot catch-up, a 2,000-seed chaos campaign, a regression seed, every injected bug (with and without the invariant checks), shrinking, and a three-node TCP cluster that loses and regains its leader. A second group of tests was added in review: Raft edge cases (stale and duplicated messages, the vote comparison in every direction, a snapshot older than the commit index, a snapshot that ends at a matching entry, ReadIndex on a leader whose no-op is not committed yet, election timer resets, client sessions after a snapshot install), a second brute-force reference for the checker that shares no code with it, hand-made histories with cas and unknown outcomes, and checks that simulated partitions, duplicates and fault counts are real.

When a seed fails, the chaos command prints the failure (the invariant and the event that broke it, or the per-key history the checker could not linearize) and a repro command. With `--shrink` it drops faults one at a time while the run still fails and prints the shrunk list, for example `lattice chaos --seed 7 --bug stale-read --faults 0,1 --trace`.

## Measured results

All numbers were measured on one Windows 11 laptop (8 logical CPUs, SSD, .NET 10.0.12, Release build) on 2026-11-18, after the review changes described in [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md#8-problems-that-were-hard-to-get-right). Other heavy jobs were running on the same machine during some of the runs, so every timing below (wall time, throughput, latency, failover, simulator speed) varied from run to run and is given as a range or marked as a single run. The correctness results do not depend on timing: they come from the simulator and are the same on any machine for the same seeds.

### Chaos campaign

Command: `lattice chaos --seeds 10000 --parallel 3` (seeds 1-10,000, invariants and linearizability checks on, no bugs), and the same on 6,000 seeds far from the ones used during development (`--start 5000001`).

| | Seeds 1-10,000 | Seeds 5,000,001-5,006,000 |
|---|---:|---:|
| Failing seeds | 0 | 0 |
| Simulated time | 22.2 hours (8 s per seed) | 13.3 hours |
| Events processed | 82,235,127 | 49,193,922 |
| Client operations | 2,734,790 (2,701,784 answered, 33,006 unknown outcome) | 1,639,314 (1,619,477 answered, 19,837 unknown) |
| Faults scheduled | 189,186 | 114,177 |
| Faults that took effect | 150,547 | 90,595 |
| Leader elections | 55,764 | 33,683 |
| Entries committed | 2,106,282 | 1,261,353 |
| Messages sent / dropped | 41,288,818 / 3,714,910 | 24,645,083 / 2,211,608 |
| Checker search steps | 7,066,091 | 4,204,609 |
| Wall time (3 threads) | 28 s | 18 s |

About a fifth of the scheduled faults find nothing to act on (crashing a node that is already down, isolating or pausing the leader while there is none, a new-leader kill that no election triggers), which is why both counts are shown.

Before the review changes, a 30,000-seed run (seeds 1-30,000) also had 0 failures. Earlier in development a 20,000-seed campaign found one real bug with nothing injected (seed 18332): a leader that had restarted could count a reply to a message from its previous incarnation as a fresh ReadIndex acknowledgement and serve a stale read while partitioned. It is fixed, explained in [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md#8-problems-that-were-hard-to-get-right), and the seed is a regression test. Putting the old reply format back makes seed 18332 fail again with a linearizability violation (checked in review).

### Bug injection

Command: `lattice bugs --seeds 2000 --parallel 3`, plus `lattice bugs --only commit-old-term --seeds 20000 --parallel 3`. Each bug is switched on alone and the same seeds are run twice: once with every check, and once with the Raft invariant checks off, so that only the linearizability checker and the end-of-run check that replicas with the same applied index hold the same state ("convergence") can catch it.

| Bug | All checks: first failing seed | All checks: failing seeds | Caught by | Checker only: first failing seed | Checker only: failing seeds (linearizability + convergence) |
|---|---:|---:|---|---:|---:|
| vote-no-log-check: vote without the log up-to-date check | 1 | 1,515 / 2,000 | leader completeness 1,511, state machine safety 4 | 3 | 830 / 2,000 (686 + 144) |
| commit-old-term: commit earlier-term entries by counting replicas | 2,508 | 9 / 20,000 | leader completeness 9 | not caught | 0 / 20,000 |
| skip-fsync: reply before fsync | 4 | 224 / 2,000 | leader completeness 218, election safety 6 | 34 | 70 / 2,000 (59 + 11) |
| stale-read: ReadIndex without the leadership check | 7 | 106 / 2,000 | linearizability 106 | 7 | 106 / 2,000 (106 + 0) |
| no-dedupe: apply retried commands again | 1 | 1,081 / 2,000 | linearizability 1,081 | 1 | 1,081 / 2,000 (1,081 + 0) |

On fresh seeds (`--start 7000001` for 2,000 seeds, `--start 3000001` for 20,000 seeds of commit-old-term) the picture is the same: first catches with all checks at seeds 1, 1,360, 16, 8 and 1; with the checker only at seeds 1, 4,265 (convergence, 1 of 20,000; linearizability never), 44, 8 and 1. commit-old-term failed 14 of 20,000 fresh seeds.

How to read this:

- Every bug is caught. Four of the five are caught within the first 50 seeds even with the Raft invariants switched off (34 on seeds 1-2,000, 44 on the fresh seeds), so the end-to-end checks do not depend on Raft-specific knowledge.
- commit-old-term (the Figure 8 mistake from the Raft paper) is the hard one: about one seed in 1,400 to 2,200. It only matters when old entries reach a majority before the new leader's own no-op does, which needs one-entry batches and a leader that dies within milliseconds of winning. The linearizability checker never sees it on its own, because the entries that get lost are ones no client was ever told about; the convergence check caught it once in 40,000 seeds. A scripted test replays Figure 8 step by step, and the test suite searches from seed 2508.
- The fault mix is not neutral: it was tuned while hunting this bug (more faults per seed, and a "crash the next new leader within 20 ms of winning" fault), which raised the commit-old-term rate from about 1 seed in 7,000 to about 1 in 2,000. The clean campaigns use the same tuned mix. See [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md#8-problems-that-were-hard-to-get-right).
- stale-read and no-dedupe leave the Raft log intact, so only the linearizability checker can catch them.
- Shrinking makes failures small: `lattice chaos --seed 7 --bug stale-read --shrink` reduces 28 faults to 2 (extra network latency, then a partition that cuts off the leader's node) and prints `lattice chaos --seed 7 --bug stale-read --faults 0,1 --trace`. The shrinker is greedy, so the result is a list from which no single fault can be dropped, not necessarily the smallest possible one.
- The checker also rejects hand-made non-linearizable histories (`CheckerTests`, `CheckerReviewTests`), agrees with a brute-force search on 3,000 random single-key histories, and agrees with a second brute-force reference that shares no code with it on 3,000 random two-key histories.

### Benchmarks (vary with machine load)

Command: `lattice bench`. Three node processes on localhost, each with its own data directory, fsync on every batch, reads via ReadIndex, 10 seconds per load test. Four runs on 2026-11-18. The third ran while another job was saturating the same disk; the put ranges leave it out and show it in brackets.

| Workload | Throughput (range over 4 runs) | p50 | p99 |
|---|---:|---:|---:|
| put, 32 concurrent clients | 4,600-5,700 ops/s (1,500 under heavy disk load) | 3.3-4.0 ms | 32-45 ms |
| get (ReadIndex), 32 concurrent clients | 20,000-39,000 ops/s | 0.7-1.3 ms | 2.2-6.5 ms |
| put, 1 client | 415-705 ops/s (78 under heavy disk load) | 1.3-1.8 ms | 3-12 ms |

| Failover (leader process killed) | Median time to a new leader | Median time to the first accepted write | Worst first write |
|---|---:|---:|---:|
| Run 1 (15 kills) | 298 ms | 420 ms | 687 ms |
| Run 2 (15 kills) | 233 ms | 437 ms | 535 ms |
| Run 4 (30 kills) | 307 ms | 372 ms | 829 ms |
| Small state, 40 kills | 393 ms | 474 ms | 707 ms |

In the third run, the 14th kill was followed by a write that got no answer for 20 s, and the benchmark stopped there. The cause turned out to be outside Lattice: a benchmark of another project on the same machine deleted the scratch directory that held this run's node data while the cluster was running (that tool's bug has since been fixed). A later run on a quiet machine killed the leader 40 times in a row with no stall: new leader median 315 ms (147 to 605 ms), first accepted write median 444 ms (169 to 749 ms).

| Simulator speed (one thread, 200 seeds) | |
|---|---:|
| Chaos seeds per second (8 simulated seconds each, all checks included) | 72-99 |
| Events per second | 0.60-0.83 million |
| Simulated seconds per wall second | 578-789 |

The benchmark also records a 5-second mixed workload (8 clients, 3 keys) against the real cluster and runs the linearizability checker on it: between 4,979 and 11,973 operations per run, linearizable every time.

What the benchmark numbers mean:

- This is localhost on one laptop: no network latency, three processes sharing one SSD and 8 CPUs. It shows that the implementation is not slow. It does not tell you what a real deployment would do.
- Writes are bound by the two fsyncs on the critical path (leader, then follower) and by one event-loop thread per node. Group commit lets 32 clients share flushes, which is why 32 clients get several times the throughput of one. The p99 for writes includes snapshots, which are taken synchronously on the event loop.
- Reads skip the disk and the log, so they are several times faster than writes.
- Failover is bounded below by the 150-300 ms randomized election timeout. Many runs skip a term number, which points at split votes between the two surviving nodes, and at the restarted node from the previous run forcing an election when it rejoins with a higher term (Lattice has no pre-vote).

## Limitations

- No membership changes. The cluster's node list is fixed at start.
- No pre-vote. A node that was partitioned away comes back with a higher term and forces an election, which costs availability (not safety).
- Snapshots are sent whole in one message, not in chunks, and are taken synchronously on the event-loop thread. That is fine for a small map but would not hold up for gigabytes.
- The log is compacted right up to the applied index with no tail kept, so a follower that misses a batch just before a compaction gets the whole snapshot instead of a few entries. etcd keeps a tail of entries for this.
- Client sessions never expire, so the session table grows with the number of client ids ever seen. Each `lattice put` (or get, cas, delete) from the command line is a new session.
- Lattice does not protect itself against its data directory being deleted while it runs. That is how one benchmark run stalled for 20 s (another tool on the machine removed the directory); 40 later failovers on a quiet machine had no stall.
- One outstanding command per client session, as in the dissertation. Concurrency comes from many sessions.
- The TCP transport has no authentication or encryption and is meant for localhost.
- The simulator tests the Raft code, the state machine and the storage interface, but not the TCP transport or `FileDurableStore` under faults. Those are covered by unit tests, a TCP smoke test and the real-cluster history check in the benchmark, not by fault injection.
- The crash model of the file store relies on `FlushFileBuffers`/fsync and on atomic rename (the directory is not fsynced after a rename, which Linux would need). Process kills are tested; power loss is not. The simulated disk loses a random suffix of the unsynced writes but never reorders them.
- The checker's search is exponential in the worst case. It has a step budget and reports "inconclusive" when it runs out, which has not happened on correct runs.
- Tested only on Windows 11 with .NET 10.

## References

- Diego Ongaro and John Ousterhout. In Search of an Understandable Consensus Algorithm. USENIX ATC 2014.
- Diego Ongaro. Consensus: Bridging Theory and Practice. PhD dissertation, Stanford University, 2014.
- Maurice Herlihy and Jeannette Wing. Linearizability: A Correctness Condition for Concurrent Objects. ACM TOPLAS 12(3), 1990.
- Jeannette Wing and Chun Gong. Testing and Verifying Concurrent Objects. Journal of Parallel and Distributed Computing 17, 1993.
- Gavin Lowe. Testing for Linearizability. Concurrency and Computation: Practice and Experience 29(4), 2017.
- Alex Horn and Daniel Kroening. Faster Linearizability Checking via P-Compositionality. FORTE 2015.

See [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md) for how each part works, why each injected bug is caught, and likely interview questions.

## License

MIT, see [LICENSE](LICENSE).
