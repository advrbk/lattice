using Lattice.Kv;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Tests;

/// <summary>
/// Edge cases of Figure 2 and section 5.4 of the Raft paper, written during review: stale and
/// duplicated messages, the vote comparison, InstallSnapshot corner cases, ReadIndex on a new leader,
/// election timer resets and sessions that must survive a snapshot install.
/// </summary>
public class RaftEdgeCaseTests
{
    private static LogEntry Put(long term, long index, string key, string value, long client = 1, long seq = 1) =>
        new(term, index, new KvCommand(KvOp.Put, client, seq, key, value, null).Encode());

    [Fact]
    public void Vote_compares_last_term_first_then_length()
    {
        // Our log: (1), (2), (2). Last term 2, last index 3.
        var h = new NodeHarness(disk: NodeHarness.DiskWith(2, (1, "a"), (2, "b"), (2, "c")));

        // Same last term, shorter log: refused.
        h.Deliver(new RequestVote(2, 1, 3, LastLogIndex: 2, LastLogTerm: 2));
        Assert.False(h.Transport.Take<RequestVoteReply>().Single().Granted);

        // Higher last term wins even with a shorter log.
        h.Deliver(new RequestVote(3, 1, 4, LastLogIndex: 1, LastLogTerm: 3));
        Assert.True(h.Transport.Take<RequestVoteReply>().Single().Granted);

        // Same last term and same length: up to date, granted (new term, so a new vote is allowed).
        h.Deliver(new RequestVote(2, 1, 5, LastLogIndex: 3, LastLogTerm: 2));
        Assert.True(h.Transport.Take<RequestVoteReply>().Single().Granted);

        // Longer log but a lower last term: refused.
        h.Deliver(new RequestVote(3, 1, 6, LastLogIndex: 9, LastLogTerm: 1));
        Assert.False(h.Transport.Take<RequestVoteReply>().Single().Granted);
    }

    [Fact]
    public void Vote_granted_after_a_term_change_is_on_disk_before_the_reply()
    {
        var h = new NodeHarness();
        h.Deliver(new RequestVote(3, 1, 7, 0, 0));
        Assert.True(h.Transport.Take<RequestVoteReply>().Single().Granted);
        Assert.Equal(new[] { "sync", "send RequestVoteReply" }, h.Log.Events);
        var restarted = new NodeHarness(disk: h.Disk);
        Assert.Equal(7, restarted.Node.Term);
        Assert.Equal(3, restarted.Node.VotedFor);
    }

    [Fact]
    public void A_vote_reply_from_an_earlier_term_is_not_counted()
    {
        var h = new NodeHarness();
        h.TickUntil(() => h.Node.Role == Role.Candidate && h.Node.Term == 1);
        h.TickUntil(() => h.Node.Term == 2); // the first election timed out
        Assert.Equal(Role.Candidate, h.Node.Role);
        h.Deliver(new RequestVoteReply(2, 1, 1, true)); // late grant for term 1
        Assert.Equal(Role.Candidate, h.Node.Role);
        h.Deliver(new RequestVoteReply(2, 1, 2, true));
        Assert.Equal(Role.Leader, h.Node.Role);
    }

    [Fact]
    public void Leader_steps_down_on_a_reply_with_a_higher_term_and_persists_it()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        long term = h.Node.Term;
        h.Deliver(new AppendEntriesReply(2, 1, term + 3, false, 0, 1, 0));
        Assert.Equal(Role.Follower, h.Node.Role);
        Assert.Equal(term + 3, h.Node.Term);
        Assert.Equal(term + 3, h.Disk.DurableTerm);
        Assert.Equal(0, h.Node.VotedFor);
    }

    [Fact]
    public void Duplicate_append_changes_nothing_and_is_acknowledged_again()
    {
        var h = new NodeHarness();
        var entries = new[] { Put(1, 1, "k", "a"), Put(1, 2, "k", "b") };
        var ae = new AppendEntries(2, 1, 1, 0, 0, entries, 0, 1);
        h.Deliver(ae);
        var first = h.Node.Log.Get(2);
        h.Log.Events.Clear();
        h.Deliver(ae);
        Assert.Same(first, h.Node.Log.Get(2));
        Assert.Equal(2, h.Node.LastLogIndex);
        Assert.Equal(new[] { "send AppendEntriesReply" }, h.Log.Events); // nothing new to sync
        Assert.All(h.Transport.Take<AppendEntriesReply>(), r => Assert.Equal(2, r.MatchIndex));
    }

    [Fact]
    public void Follower_commit_index_never_passes_the_last_entry_this_append_verified()
    {
        // The follower holds stale entries 2..3 from term 1 that the leader of term 2 has not replaced yet.
        var h = new NodeHarness(disk: NodeHarness.DiskWith(1, (1, "a"), (1, "stale"), (1, "stale")));
        // A heartbeat that only proves index 1 matches, with the leader's commit index at 3.
        h.Deliver(new AppendEntries(2, 1, 2, 1, 1, Array.Empty<LogEntry>(), LeaderCommit: 3, Seq: 1));
        Assert.Equal(1, h.Node.CommitIndex);
        Assert.Equal(3, h.Node.LastLogIndex); // nothing conflicted, so nothing was truncated
    }

    [Fact]
    public void A_stale_success_reply_does_not_move_match_or_next_backwards()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        long term = h.Node.Term;
        for (int i = 0; i < 3; i++) h.Node.Propose(NodeHarness.Command($"v{i}", seq: i + 1), _ => { });
        h.Node.Flush();
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 4, 0, 0));
        Assert.Equal(4, h.Node.CommitIndex);

        h.Deliver(new AppendEntriesReply(2, 1, term, true, 2, 0, 0)); // reordered, older acknowledgement
        Assert.Equal(4, h.Node.CommitIndex);
        h.Transport.Sent.Clear();
        h.Node.Propose(NodeHarness.Command("v3", seq: 4), _ => { });
        h.Node.Flush();
        var toNode2 = h.Transport.Take<AppendEntries>().Where(m => m.To == 2).ToList();
        Assert.NotEmpty(toNode2);
        Assert.All(toNode2, m => Assert.True(m.PrevLogIndex >= 4, $"resent from {m.PrevLogIndex + 1}"));
    }

    [Fact]
    public void A_stale_rejection_does_not_reset_a_peer_that_already_matches()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        long term = h.Node.Term;
        h.Node.Propose(NodeHarness.Command("v"), _ => { });
        h.Node.Flush();
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 2, 0, 0));
        h.Transport.Sent.Clear();
        // A rejection of an old probe (prev index 0) arrives late: node 2 is known to match up to 2.
        h.Deliver(new AppendEntriesReply(2, 1, term, false, 0, 1, 0, RejectedPrevIndex: 0));
        Assert.DoesNotContain(h.Transport.Take<AppendEntries>(), m => m.To == 2 && m.PrevLogIndex < 2);
    }

    [Fact]
    public void Heartbeat_after_compaction_does_not_send_a_snapshot_to_a_follower_that_is_only_in_flight()
    {
        var h = new NodeHarness(snapshotThreshold: 4);
        h.BecomeLeader();
        long term = h.Node.Term;
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 1, 0, 0));
        h.Deliver(new AppendEntriesReply(3, 1, term, true, 1, 0, 0));
        for (int i = 0; i < 5; i++) h.Node.Propose(NodeHarness.Command($"v{i}", seq: i + 1), _ => { });
        h.Node.Flush(); // entries 2..6 go to both followers

        // Node 3 acknowledges them, node 2's acknowledgement is still in flight. The leader commits 6 and
        // compacts its log into a snapshot at 6, past what it knows node 2 has.
        h.Deliver(new AppendEntriesReply(3, 1, term, true, 6, 0, 0));
        Assert.Equal(6, h.Node.SnapshotIndex);
        h.Transport.Sent.Clear();
        h.TickUntil(() => h.Transport.Sent.Any(m => m.To == 2));
        var toNode2 = h.Transport.Sent.Where(m => m.To == 2).ToList();
        Assert.DoesNotContain(toNode2, m => m is InstallSnapshot);
        Assert.Contains(toNode2, m => m is AppendEntries { PrevLogIndex: 6 });

        // If node 2 really lost the batch it rejects; only then does it get the snapshot.
        h.Transport.Sent.Clear();
        h.Deliver(new AppendEntriesReply(2, 1, term, false, 0, 2, 0, RejectedPrevIndex: 6));
        Assert.Contains(h.Transport.Sent, m => m is InstallSnapshot { To: 2, LastIndex: 6 });
    }

    [Fact]
    public void A_snapshot_older_than_the_commit_index_is_ignored()
    {
        var h = new NodeHarness();
        var entries = new[] { Put(1, 1, "k", "a", seq: 1), Put(1, 2, "k", "b", seq: 2), Put(1, 3, "k", "c", seq: 3) };
        h.Deliver(new AppendEntries(2, 1, 1, 0, 0, entries, LeaderCommit: 3, Seq: 1));
        Assert.Equal(3, h.Node.CommitIndex);
        Assert.Equal("c", h.Sm.Get("k"));

        var old = new KvStateMachine();
        old.Apply(new KvCommand(KvOp.Put, 1, 1, "k", "a", null));
        h.Deliver(new InstallSnapshot(2, 1, 1, 1, 1, old.TakeSnapshot(), 2));
        Assert.Equal(0, h.Node.SnapshotIndex);
        Assert.Equal(3, h.Node.LastLogIndex);
        Assert.Equal("c", h.Sm.Get("k")); // the state machine did not go back in time
        Assert.Equal(1, h.Transport.Take<InstallSnapshotReply>().Last().LastIndex);
    }

    [Fact]
    public void A_snapshot_ending_at_a_matching_entry_keeps_the_log_after_it()
    {
        var h = new NodeHarness(disk: NodeHarness.DiskWith(1, (1, "a"), (1, "b"), (1, "c"), (1, "d")));
        var sm = new KvStateMachine();
        sm.Apply(new KvCommand(KvOp.Put, 1, 1, "k", "b", null));
        h.Deliver(new InstallSnapshot(2, 1, 1, 2, 1, sm.TakeSnapshot(), 1));
        Assert.Equal(2, h.Node.SnapshotIndex);
        Assert.Equal(4, h.Node.LastLogIndex);
        Assert.Equal(2, h.Node.CommitIndex);

        var restarted = new NodeHarness(disk: h.Disk);
        Assert.Equal(2, restarted.Node.SnapshotIndex);
        Assert.Equal(4, restarted.Node.LastLogIndex);
        Assert.Equal("b", restarted.Sm.Get("k"));
    }

    [Fact]
    public void A_snapshot_that_conflicts_with_the_log_discards_it()
    {
        var h = new NodeHarness(disk: NodeHarness.DiskWith(1, (1, "a"), (1, "b"), (1, "c")));
        h.Deliver(new InstallSnapshot(2, 1, 2, 2, 2, new KvStateMachine().TakeSnapshot(), 1));
        Assert.Equal(2, h.Node.SnapshotIndex);
        Assert.Equal(2, h.Node.LastLogIndex);
        Assert.Equal(2, h.Node.LastLogTerm);
        Assert.Equal(2, h.Disk.DurableLog.LastIndex);
    }

    [Fact]
    public void Client_sessions_survive_a_snapshot_install_on_a_follower()
    {
        // The snapshot already applied client 5's put (seq 3). Afterwards client 9 overwrites the key, and
        // then a retry of client 5's seq 3 arrives through the log. It must not apply again.
        var leaderSm = new KvStateMachine();
        leaderSm.Apply(new KvCommand(KvOp.Put, 5, 3, "x", "from-5", null));
        var h = new NodeHarness();
        h.Deliver(new InstallSnapshot(2, 1, 1, 10, 1, leaderSm.TakeSnapshot(), 1));
        var entries = new[] { Put(1, 11, "x", "from-9", client: 9, seq: 1), Put(1, 12, "x", "from-5", client: 5, seq: 3) };
        h.Deliver(new AppendEntries(2, 1, 1, 10, 1, entries, LeaderCommit: 12, Seq: 2));
        Assert.Equal(12, h.Node.LastApplied);
        Assert.Equal("from-9", h.Sm.Get("x"));
    }

    [Fact]
    public void New_leader_serves_ReadIndex_only_after_its_own_noop_commits()
    {
        // Index 1 holds an entry from an earlier term that this node does not know to be committed.
        var h = new NodeHarness(disk: NodeHarness.DiskWith(1, (1, "old")));
        h.BecomeLeader();
        long term = h.Node.Term;
        Assert.Equal(0, h.Node.CommitIndex);
        h.Transport.Sent.Clear();

        ReadStatus? status = null;
        h.Node.ReadIndex((s, _) => status = s);
        h.Node.Flush();
        long seq = h.Transport.Take<AppendEntries>().Max(m => m.Seq);

        // Leadership is confirmed by a fresh reply, but node 2 only holds index 1: the no-op is not committed.
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 1, 0, seq));
        Assert.Equal(0, h.Node.CommitIndex);
        Assert.Null(status);

        h.Deliver(new AppendEntriesReply(2, 1, term, true, 2, 0, seq));
        Assert.Equal(2, h.Node.CommitIndex);
        Assert.Equal(ReadStatus.Ready, status);
    }

    [Fact]
    public void ReadIndex_is_not_confirmed_by_replies_to_messages_sent_before_the_read()
    {
        var h = new NodeHarness(clusterSize: 5);
        h.TickUntil(() => h.Node.Role == Role.Candidate);
        long term = h.Node.Term;
        h.Deliver(new RequestVoteReply(2, 1, term, true));
        h.Deliver(new RequestVoteReply(3, 1, term, true));
        Assert.Equal(Role.Leader, h.Node.Role);
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 1, 0, 0));
        h.Deliver(new AppendEntriesReply(3, 1, term, true, 1, 0, 0));
        Assert.Equal(1, h.Node.CommitIndex);
        h.Transport.Sent.Clear();

        ReadStatus? status = null;
        h.Node.ReadIndex((s, _) => status = s);
        h.Node.Flush();
        long seq = h.Transport.Take<AppendEntries>().Max(m => m.Seq);

        // Two peers answer older messages, one answers the new round: 1 + 1 fresh acknowledgements of 5.
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 1, 0, seq - 1));
        h.Deliver(new AppendEntriesReply(3, 1, term, true, 1, 0, seq - 1));
        h.Deliver(new AppendEntriesReply(4, 1, term, true, 1, 0, seq));
        Assert.Null(status);
        // A rejection still proves the peer is in our term, so it counts as an acknowledgement.
        h.Deliver(new AppendEntriesReply(5, 1, term, false, 0, 2, seq, RejectedPrevIndex: 99));
        Assert.Equal(ReadStatus.Ready, status);
    }

    [Fact]
    public void Election_timer_is_reset_by_the_current_leader_but_not_by_stale_messages()
    {
        var h = new NodeHarness();
        h.Deliver(new RequestVote(2, 1, 5, 0, 0)); // now in term 5
        for (int i = 0; i < 200; i++)
        {
            if (i % 5 == 0) h.Deliver(new AppendEntries(2, 1, 5, 0, 0, Array.Empty<LogEntry>(), 0, i + 1));
            h.Node.Tick();
            h.Node.Flush();
        }
        Assert.Equal(Role.Follower, h.Node.Role);
        Assert.Equal(5, h.Node.Term);

        // Messages from a deposed leader of term 4 must not keep this node from starting an election.
        h.TickUntilWith(() => h.Node.Role == Role.Candidate,
            () => h.Deliver(new AppendEntries(3, 1, 4, 0, 0, Array.Empty<LogEntry>(), 0, 1)));
        Assert.Equal(6, h.Node.Term);
    }
}

internal static class NodeHarnessExtensions
{
    /// <summary>Ticks until <paramref name="condition"/> holds, running <paramref name="each"/> before every tick.</summary>
    public static void TickUntilWith(this NodeHarness h, Func<bool> condition, Action each, int max = 100)
    {
        for (int i = 0; i < max && !condition(); i++)
        {
            each();
            h.Node.Tick();
            h.Node.Flush();
        }
        Assert.True(condition(), "condition not reached by ticking");
    }
}
