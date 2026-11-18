# How Lattice works

This is the guide to read before talking about Lattice in an interview. It explains Raft in plain words, then the key-value layer, then how the same node code runs both in a deterministic simulator and over TCP, then how correctness is checked (Raft invariants and a linearizability checker), then each injected bug and why the tests catch it. It ends with the problems that were hard to get right and a list of likely interview questions with short answers.

## 1. Raft in plain words

A replicated key-value store keeps the same data on several machines so that it survives some of them failing. The hard part is agreeing on the order of writes when messages get lost or delayed and machines crash. Raft (Ongaro and Ousterhout, 2014) solves this by electing one leader that decides the order, and by only treating a write as done once a majority of machines have stored it.

### Terms and roles

Time is divided into numbered terms. Each term starts with an election and has at most one leader. Every node is a follower, a candidate or a leader. Every message carries the sender's term. A node that sees a higher term than its own adopts it and becomes a follower; a message with a lower term is stale and is rejected. Terms act as a logical clock that lets nodes detect stale leaders without any synchronized time.

### Elections

A follower expects to hear from the leader regularly (heartbeats). If it hears nothing for an election timeout, it assumes the leader is gone, increments its term, votes for itself and asks everyone else for votes (`RequestVote`). Each node votes at most once per term, first come first served, so two candidates cannot both get a majority in the same term. The timeout is randomized (150-300 ms here) so that usually one node times out first and wins before anyone else starts competing.

There is one more rule, the election restriction (section 5.4.1): a node refuses to vote for a candidate whose log is less up-to-date than its own. "Up-to-date" compares the term of the last log entry first and then the log length. Any committed entry is on a majority, and any two majorities overlap, so a candidate that wins must have every committed entry. This is what makes the leader safe to trust.

### Log replication

Clients send commands to the leader. The leader appends each command to its log as an entry tagged with the current term and sends it to followers in `AppendEntries`. Each `AppendEntries` also names the entry just before the new ones (`PrevLogIndex`, `PrevLogTerm`). A follower only accepts if it has that exact entry. If it does not, it rejects, and the leader backs up and tries an earlier point. When the follower accepts, it deletes anything in its log that conflicts with the leader's entries (same index, different term) and appends the new ones.

That consistency check gives the Log Matching property: if two logs have an entry with the same index and term, the logs are identical up to that entry. It holds by induction, because each append was checked against its predecessor.

Lattice's leader tracks each follower in one of three states, like etcd's implementation: probing (send one batch and wait to learn where the logs match), replicating (stream batches without waiting, up to an in-flight limit) and snapshot (the follower needs entries that were compacted away, so send the snapshot). Rejections carry a hint (the first index of the conflicting term) so the leader skips back a whole term at a time. Heartbeats to a replicating follower resend everything after its last acknowledged index, which repairs lost batches; if those entries were just compacted away, the heartbeat starts from the next unsent index instead, and the follower only gets a snapshot if it then rejects (see section 8).

### Commit and apply

An entry is committed once it is stored on a majority. Committed entries are applied to the key-value map in log order on every node, and the client gets its answer when the leader applies its entry.

There is a subtle exception, shown in Figure 8 of the paper: a leader may only count replicas for entries from its own term. An entry from an earlier term can sit on a majority and still be overwritten later, because a node with a newer but different entry at that index can still win an election. Older entries become committed indirectly, when an entry from the current term after them is committed. That is why every new leader immediately appends a no-op entry in its own term. One of the injected bugs breaks exactly this rule.

### Persistence

Three things must survive a crash: the current term, the vote cast in that term, and the log. If a node forgot its vote, it could vote twice in one term and help elect two leaders. If it forgot log entries it had acknowledged, an entry the leader counted as committed could disappear. So a node must write these to disk and fsync before it answers a vote request or acknowledges entries. In Lattice the node writes to its store during a handler and queues outgoing messages; `Flush` fsyncs and only then sends. The leader also counts itself towards a majority only for entries that are on its own disk.

The commit index and the state machine are not persisted. After a restart a node rebuilds them from the snapshot and the log once it learns the commit index from the leader.

### Snapshots

The log cannot grow forever. When enough applied entries pile up after the last snapshot, a node serializes the state machine (including the client session table) into a snapshot and drops the log prefix it covers. If a follower is so far behind that the leader no longer has the entries it needs, the leader sends the snapshot (`InstallSnapshot`), and the follower replaces its state with it. Only applied, therefore committed, entries are ever compacted.

## 2. The key-value layer

The state machine is an ordered map from string keys to string values with get, put, delete and compare-and-swap (`cas key expected new`, where "expected" can be "absent").

### Exactly-once with client sessions

Networks lose replies. If a client sends `cas x 0 1`, the cluster applies it, and the reply is lost, the client retries. Applying the retry would fail (x is now 1) and the client would be told its own successful swap failed. Worse, a retried put could overwrite a newer value.

The fix (section 6.3 of Ongaro's dissertation) is a session per client: every command carries a client id and a sequence number, the client has at most one command outstanding and retries with the same sequence number, and the state machine remembers, per client, the last sequence number it applied and its result. A command whose sequence number is not newer is a duplicate, and the remembered result is returned instead of applying it again. The session table is part of the snapshot, so the guarantee survives compaction and restarts. This also absorbs duplicates created by the network itself.

Sessions here never expire. A long-running deployment would need expiry decided through the log (so every replica expires the same sessions at the same point), plus an error for clients whose session was expired.

### Linearizable reads: ReadIndex

A read served from the leader's local copy is not safe on its own: a leader that was cut off may not know it has been replaced, and would return stale data. There are three common ways to make reads linearizable:

1. Put reads through the log like writes. Simple and obviously correct, but every read costs a log append, a disk fsync and a round of replication.
2. ReadIndex (section 6.4 of the dissertation). The leader records its current commit index as the read index, confirms it is still the leader by getting heartbeat replies from a majority sent after the read arrived, waits until it has applied up to the read index, then reads locally. No disk write.
3. Leases. The leader assumes no one else can be elected for a while after a successful heartbeat round and serves reads locally during that time. This needs bounded clock drift, which the simulator deliberately violates (clock drift and long pauses), so Lattice does not use it.

Lattice uses ReadIndex by default and supports reads through the log as an option (`--read-mode log`; the simulator picks either mode per seed so both are tested). Two details matter. First, the read index is the maximum of the commit index and the index of the leader's no-op for its term: a new leader may not yet know which earlier entries are committed, and waiting until its own no-op is applied guarantees that everything committed before it took over is included. Second, the confirmation uses a sequence number carried in every `AppendEntries` and echoed in the reply. A read needs a majority of replies to messages sent after the read arrived; replies to older messages prove nothing about the present. Reads that arrive in the same event-loop batch share one heartbeat round.

## 3. One node, two transports

`RaftNode` is a single-threaded state machine with no threads, sockets or clocks of its own. The host drives it through `Tick()` (one logical clock tick), `Receive(message)`, `Propose(command)` and `ReadIndex()`, and then calls `Flush()`. Messages go out through `IRaftTransport.Send`. There are two implementations of that interface:

- The simulator's network, which delivers messages through a seeded event queue and injects faults.
- `ServerHost`'s TCP transport, where each node is a separate process. Every peer has one outgoing connection with its own queue and writer task; messages are dropped if a connection fails, because Raft retries on its own. A single event-loop thread owns the node: socket readers only decode frames and post them to the loop. After draining a batch of events the loop calls `Flush` once, so one fsync covers every write in the batch (group commit). That is what lets 32 concurrent clients share disk flushes.

Durable storage has two implementations of `IDurableStore` as well. `FileDurableStore` keeps `hardstate` (term and vote) and `snapshot` as small files replaced atomically (write a temp file, fsync, rename), and the log as an append-only file of CRC-protected records. A torn or corrupt record ends the log on load and the file is cut back to the last good record. Saving a snapshot rewrites the log without the compacted prefix; a crash between writing the snapshot and rewriting the log is handled on load. `SimDisk` is the simulated version described below.

## 4. The deterministic simulator

Distributed bugs depend on timing: which message arrives first, whether a node crashes before or after an fsync. Testing on real machines finds such bugs by luck and cannot replay them. The simulator runs a whole cluster, its clients and its faults on one thread, driven by one priority queue of timed events, with every random choice drawn from generators seeded by the run's seed. A run is therefore a pure function of its seed: the same seed processes the same events in the same order, and a failure can be replayed exactly, traced and shrunk. A test checks this by comparing a hash of every processed event (and the final state) between two runs.

What is simulated:

- Clocks. Each node ticks every 10 ms of simulated time, with its own clock drift (up to 20% fast or slow, chosen again on every restart) and random jitter on every tick.
- Network. Every message gets a random delay. Messages can be dropped, duplicated, or delayed by up to 60 ms extra so that they arrive out of order. Partitions cut links between servers in both directions, including messages already in flight. Clients talk to servers over the same network (with the same delays, loss and duplication), but partitions never cut a client off from a server: that is what lets a deposed leader that is still reachable by clients be tempted to serve a stale read.
- Disks. `SimDisk` buffers writes like an OS page cache. `Sync` makes them durable. A background "writeback" flushes them at random intervals, like an OS would. A crash keeps a random prefix of the unsynced writes and loses the rest.
- Crashes and restarts. A crash discards the node object entirely (all volatile state); a restart builds a new node from what is durable on its disk.
- Pauses. A paused node stops ticking and processing messages (like a long GC pause or a frozen VM), then resumes with everything that queued up.

Each seed also chooses the cluster size (3 or 5), 3 to 6 clients, 1 to 4 keys, the network quality, the batch size (1 to 64 entries per message), the in-flight limit, the snapshot threshold, whether check-quorum is on, and the read mode. The fault plan for a seed has 8 to 30 faults: partitions of random minorities, isolating the current leader, crashing a random node or the current leader, crashing the next newly elected leader a few milliseconds after it wins, pausing a node or the leader, and bursts of loss, duplication or latency. Every fault undoes itself after a random duration. At the end of the workload every fault is healed, every node restarted, and clients get three more simulated seconds to finish.

Clients each run one operation at a time (40% get, 30% put, 10% delete, 20% cas), every write value is unique, and a cas uses the last value that client saw, so a fair share succeed. A client retries on timeouts and "not leader" answers with the same sequence number and gives up after 2 s, recording the outcome as unknown.

The fault plan uses its own random stream, separate from the run's. So the shrinker can drop one fault and re-run: if the run still fails, the fault was not needed. The result is a short repro command. For example, `lattice chaos --seed 7 --bug stale-read --shrink` cuts 28 faults down to 2 (a burst of extra latency and a partition that isolates node 5) and prints `lattice chaos --seed 7 --bug stale-read --faults 0,1 --trace`.

## 5. Checking Raft's safety properties

`RaftInvariants` is an observer that every node calls when it becomes leader, appends, truncates, commits and applies. It checks the properties of Figure 3 of the paper at those steps rather than after every simulator event. A crash that loses data, for example, is reported at the next election or commit that depends on the lost data. A failure therefore points at the step where a property first broke:

- Election safety: at most one leader per term.
- Log matching: two entries with the same index and term have the same command and the same preceding term.
- Leader completeness: a node that becomes leader of term T has every entry that was committed in a term before T.
- State machine safety: no two nodes commit or apply different entries at the same index.
- No node truncates an entry at or below its own commit index.

At the end of a run, replicas that applied the same number of entries must also have byte-identical state (data and sessions).

## 6. The linearizability checker

### What linearizable means

Linearizability (Herlihy and Wing, 1990) says the system behaves like a single copy of the data where each operation takes effect at one instant between the moment the client sent it and the moment the client got the answer. If operation A finished before operation B started, A must take effect first. If they overlap, either order is fine, but the results must match some single order.

A history is a list of operations, each with an invoke time, a return time and the result the client saw. Operations whose answer never arrived (timeouts, crashes) have an unknown result and no return time: they may have taken effect at any point after they were invoked, or never.

### The search

The checker is the Wing and Gong (1993) algorithm with the memoization of Lowe (2017), as used by tools like Porcupine. It puts every invoke and return event in a list sorted by time and walks it:

- At an invoke event, it tries to linearize that operation now: apply it to a model of a single register and see whether the result matches what the client saw. If it matches, and the pair (set of operations linearized so far, model state) has not been seen before, it commits to that choice, removes the operation from the list and starts again from the front.
- At a return event, the operation it belongs to should have been linearized before its own return. If it was not, the last choice was wrong: undo it and try the next candidate.

If the list empties, the history is linearizable. If it has to undo past the first choice, it is not. The cache of (linearized set, state) pairs is what makes this practical: two different orders that reach the same set and the same value are the same situation, so it is explored once.

### Per-key partitioning

Every operation touches one key, and a key-value map is a set of independent registers. Linearizability is compositional (P-compositionality, Horn and Kroening 2015): the whole history is linearizable exactly when each per-key sub-history is. So each key is checked separately, which keeps each search small.

### The model and unknown results

The model of one key is a value or nothing. A get must return the current value. A put or delete always succeeds. A cas succeeds exactly when the current value equals the expected one; when it fails, it also reports the value it found, and that must be the current one. An operation with an unknown result accepts any outcome: an unknown put may be applied at any point after its invocation, and since its return time is infinite it can always be placed at the very end, which is the same as never.

Unknown writes are what make the search expensive: each stays "open" until the end of the history and doubles the configurations the search can visit. The checker removes unknown puts and cas operations whose new value nobody ever observed (no read returned it, no cas expected it, no failed cas found it). This is safe: if nobody observed the value, the operation can always be moved to the end of a valid linearization without changing anything else, and conversely. Unknown reads constrain nothing and are dropped too. A random test compares the checker with a brute-force search over all orders on 3,000 small single-key histories to make sure the pruning and the search agree. That brute force reuses the checker's model of one key, so a second test (`CheckerReviewTests`) uses a reference that shares no code with the checker: its own model of the whole map, applied to 3,000 random two-key histories at once, which also tests the per-key split. Another test builds linearizable histories from a sequential run, confirms the checker accepts them, then corrupts one read and confirms it rejects them.

The search has a step budget (500,000 per key). Exceeding it is reported as "inconclusive", not as a pass and not as a bug found. In practice correct histories take a few hundred steps per key.

## 7. The injected bugs and why they are caught

Each bug is a flag (`lattice chaos --bug <name>`). The bug table in the README gives the measured seeds-to-first-catch.

### vote-no-log-check: grant votes without the up-to-date check

Without the election restriction, a node whose log is missing committed entries can win an election, and as leader it will overwrite those entries on the followers. The invariant checker sees it at the moment of election (Leader Completeness). With invariants off, the linearizability checker sees it as lost writes: a read returns a value that had already been overwritten by an acknowledged write, or a cas fails with a value that should be gone. It is caught within a handful of seeds because any election after a partition or crash will do.

### commit-old-term: commit earlier-term entries by counting replicas

This is Figure 8. The new leader counts replicas for an entry from an earlier term and commits it as soon as it sits on a majority. If the leader then crashes before an entry from its own term reaches that majority, a node with a different, newer entry at that index can win the next election and overwrite the "committed" entry. A test (`Figure8Tests`) replays the figure step by step on a hand-driven five-node cluster and shows the correct code refuses to commit while the buggy code commits and then loses the entry.

In random testing it is the hardest bug to hit. Because every leader appends a no-op at the start of its term, the bug only matters when old entries reach a majority before the no-op does. That needs small batches (one entry per message in the failing seeds), a leader with a long uncommitted tail left over from an earlier term, and that leader dying within milliseconds of winning, before its no-op spreads. The "kill the new leader" fault exists because of this analysis. The chaos test catches it in about one seed in 1,400 to 2,200 (9 of seeds 1-20,000, the first being seed 2508, and 14 of 20,000 fresh seeds starting at 3,000,001), and the test suite starts its search at seed 2508.

The linearizability checker does not catch this bug on its own in 40,000 seeds (the end-of-run convergence check caught it once), and the reason is instructive. The entries that get "committed" and then lost come from an earlier term, so no client is waiting on them at the new leader: their clients were told "outcome unknown" when the old leader stepped down, and retried. To the checker they are writes with an unknown outcome, which may legitimately never have happened. Only the invariant checker, which knows what Raft considered committed, can see the violation.

### skip-fsync: reply before the term, vote and log are on disk

The node sends votes and acknowledgements without syncing. The simulated OS writes buffered data back after a random delay, so most of the time nothing goes wrong, but a crash in that window loses a vote or acknowledged entries. A node that forgot its vote can vote again in the same term (two leaders in one term, Election Safety), and a node that forgot acknowledged entries breaks the majority that made an entry committed (Leader Completeness). It is caught because crashes are frequent and the crash model keeps only a random prefix of unsynced writes.

### stale-read: serve reads without confirming leadership

The leader serves a read as soon as it has applied up to the read index, without the heartbeat round. A leader that was paused or partitioned, and has been replaced without knowing it, returns old data. The Raft invariants cannot see this (the log is fine), but the linearizability checker does: a read returns a value older than a write that completed before the read started. The "pause the leader" and "isolate the leader" faults create exactly that situation.

### no-dedupe: apply retried commands again

With the session check off, a retried or network-duplicated command is applied twice. The raft log and invariants are all fine, but a client whose cas was applied and whose reply was lost retries, the second application fails, and the client is told "failed, found <its own new value>". No linearization explains a cas that failed because of a value only it could have written, so the checker reports it. This is caught within a few seeds because message duplication is enabled in most runs.

### The checker on its own

The bug table also runs every bug with the Raft invariants turned off, so that only the linearizability checker and the end-of-run convergence check can catch it. The checker catches every bug except commit-old-term on its own (within 34 seeds on seeds 1-2,000 and within 44 on fresh seeds, counting the convergence check); the invariants catch the log-level bugs earlier and closer to the cause. The checker also flags hand-made non-linearizable histories in `CheckerTests` (a stale read, reads going back in time, two successful cas from the same value, a failed cas reporting the wrong value, an unknown write that would have to apply twice).

## 8. Problems that were hard to get right

- A real bug, found by the chaos test with nothing injected. A 20,000-seed campaign failed on seed 18332 with a stale read: a client saw value B, and a moment later read the older value A. The Raft invariants all held. Tracing the seed showed the read was served by a leader that had been partitioned away for 200 ms and should not have been able to confirm anything. Printing its state showed a follower's acknowledgement counter at 26 while the leader had only sent up to 22. The cause: that leader had crashed and restarted earlier, so its heartbeat sequence counter started again from 0. A delayed `AppendEntries` from its previous incarnation (old term, sequence 26) reached a follower, which correctly rejected it as stale, but the rejection carried the follower's current term, which by then was the restarted node's new term, and it echoed sequence 26. The leader counted it as a fresh acknowledgement for a read. The fix is that answers to stale-term messages never echo the sequence number; since a node can lead a given term only once, every answer that can still count is a reply to a message the current leader sent in the current term. The seed is now a regression test, and a unit test checks the reply format. A real cluster would hit this only rarely (it needs a restart, a delayed packet and a read at the wrong moment), which is exactly the kind of bug the simulator is for.
- A false alarm in the invariant checker. Early runs reported a Leader Completeness violation in which a leader of term 3 was elected after an entry was committed in term 4. The trace showed it was legitimate: the candidate had collected its votes in term 3, was paused, and processed the vote replies after the others had moved on. A stale-term leader cannot commit anything, and the paper's property is only about entries committed in earlier terms. The checker now records the earliest term in which each entry was known committed and only checks those.
- A message storm. Under the vote bug a follower can keep rejecting the leader forever. Each duplicated rejection made the leader send a fresh probe, so duplicates multiplied with every round trip and one seed ran for minutes. Rejections now echo the `PrevLogIndex` they reject (as in etcd), and the leader only reacts to the rejection of the probe it has outstanding. The simulator also has an event cap that turns any future storm into a reported failure.
- Checker blow-up. The checker ran out of memory on badly broken histories until unknown writes nobody observed were pruned (section 6) and the search got a budget.
- Making the Figure 8 bug findable. It was not found in 3,000 seeds at first. Reading a failing trace from a 20,000-seed run showed which faults it needed, and the fault mix was changed (more faults per seed, a "kill the next new leader" fault). That raised the rate from about 1 in 7,000 seeds to about 1 in 2,000, and the clean campaigns still pass.
- A performance problem on the real cluster. While a follower was in snapshot state the leader resent the whole snapshot on every heartbeat (30 ms), which with tens of thousands of keys meant megabytes per second of useless traffic. Snapshots are now resent only after an election timeout without an answer.
- Needless snapshots after compaction (found in review). The leader compacts its log up to its applied index, which is often a few entries past what the slower follower has acknowledged, because that follower's acknowledgements are still in flight. Heartbeats resent everything after the follower's last acknowledged index, found it compacted, and sent the whole snapshot to a follower that was only milliseconds behind. Now a heartbeat to a replicating follower starts from the next unsent index when the acknowledged prefix is gone; a follower that really lost a batch rejects it, drops back to probing, and gets the snapshot then. A unit test (`RaftEdgeCaseTests`) pins both halves. The simulator still sends some snapshots in healthy runs, because its random per-message delays reorder batches and a reordered batch looks like a lost one; keeping a tail of entries after each compaction, as etcd does, would remove those too.
- Counting faults honestly (found in review). The campaign used to report every fault in the schedule as "injected", but some find nothing to act on: crashing a node that is already down, isolating or pausing "the leader" while there is none, or arming a new-leader kill that no election triggers before the workload ends. The simulator now counts the faults that took effect, and the README reports both numbers.

## 9. Likely interview questions

**1. Why can't a leader commit an entry from a previous term just because a majority has it?**
Because a majority holding it does not stop a node with a different entry at that index from winning a later election: that node only needs a log with a higher last term than the voters'. Figure 8 shows the sequence. Counting only current-term entries works because once a current-term entry is on a majority, any future leader must have it (election restriction), and by Log Matching it then has everything before it too.

**2. What exactly has to be fsynced, and when?**
Term, vote and log entries, before the node sends anything that depends on them: a vote grant, an append acknowledgement, and, for the leader, before it counts itself towards a majority. Lattice enforces this structurally: handlers only queue messages, and `Flush` syncs before sending. The commit index and state machine are rebuilt after a restart.

**3. How do you make reads linearizable, and why not leases?**
ReadIndex: record the commit index (at least the leader's own no-op), confirm leadership with a majority of heartbeat replies to messages sent after the read arrived, wait to apply up to that index, read locally. Leases avoid the heartbeat round but rely on bounded clock drift; the simulator injects drift and long pauses, which is exactly what breaks leases. Reads through the log are also supported and simpler, at the cost of a disk write per read.

**4. What does exactly-once mean here, and how is it implemented?**
A command's effect happens at most once even if the client retries, and the client gets the result of that one application. Each client numbers its commands and has one outstanding at a time; the state machine stores the last sequence number and result per client and answers duplicates from that table. The table is replicated and snapshotted like the data.

**5. How does the simulator stay deterministic?**
One thread, one event queue ordered by (time, insertion number), and every random choice from a seeded xoshiro256** generator (not System.Random, whose seeded sequence is a runtime detail). The node itself has no threads or timers; time is ticks delivered by the queue. Dictionary iteration order depends only on the sequence of operations, not on hash values, so it is deterministic too. A test runs a seed twice and compares a hash of every event.

**6. How does the linearizability checker deal with operations that timed out?**
Their return time is infinity and any result is accepted, so they can be linearized at any point after their invocation or effectively never (at the end). Unknown reads are dropped. Unknown writes whose value was never observed are pruned because they can always be moved to the end.

**7. Why check each key separately? Is that sound?**
Yes, for operations that each touch one object: linearizability is a local property (Herlihy and Wing), so a history is linearizable if and only if each per-object sub-history is. The search cost is exponential in concurrency, so splitting by key is the biggest single speed-up.

**8. How do you know the tests would catch real bugs?**
The five injected bugs. Each is a plausible mistake from the paper or from practice, each is caught by the chaos test, and the seed counts are reported. Four are also caught by the linearizability checker alone, which shows the end-to-end check does not depend on the Raft-specific invariants. The checker itself is cross-checked against brute force on random histories.

**9. What happens when a partitioned old leader comes back?**
It still believes it is leader in its old term. Its first message to anyone in the new term is rejected with the new term, and it steps down. With check-quorum on, it steps down by itself after an election timeout without hearing from a majority. Anything it appended without committing is overwritten by the new leader's log. Reads it holds are failed back to clients, which retry elsewhere.

**10. Did the tests find any real bugs?**
Yes. Besides the injected ones, a campaign found a stale ReadIndex read with nothing injected (section 8): replies to stale-term messages echoed the request's sequence number, and a leader that had restarted, so its counter had reset, could mistake one for a fresh acknowledgement. The invariants held throughout; only the linearizability checker saw it. It took one command to replay the seed with a trace, which is the main payoff of a deterministic simulator.

**11. What would you add next?**
Pre-vote, so a node that was partitioned does not force a new election with a higher term when it rejoins; membership changes (single-server changes from the dissertation); chunked snapshot transfer; session expiry through the log; and real fault injection on the TCP cluster (dropping or delaying packets) to complement the simulator.

## References

- Diego Ongaro and John Ousterhout. In Search of an Understandable Consensus Algorithm. USENIX ATC 2014. (Figure 2: the protocol; Figure 3: the safety properties; Figure 8: committing entries from earlier terms.)
- Diego Ongaro. Consensus: Bridging Theory and Practice. PhD dissertation, Stanford, 2014. (Section 6.3 client sessions, section 6.4 ReadIndex and leases.)
- Maurice Herlihy and Jeannette Wing. Linearizability: A Correctness Condition for Concurrent Objects. ACM TOPLAS 12(3), 1990.
- Jeannette Wing and Chun Gong. Testing and Verifying Concurrent Objects. Journal of Parallel and Distributed Computing 17, 1993.
- Gavin Lowe. Testing for Linearizability. Concurrency and Computation: Practice and Experience 29(4), 2017.
- Alex Horn and Daniel Kroening. Faster Linearizability Checking via P-Compositionality. FORTE 2015.
