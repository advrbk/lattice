using Lattice.Kv;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Tests;

/// <summary>Single-node tests: the test plays the other servers by hand and inspects every message.</summary>
public class RaftNodeTests
{
    [Fact]
    public void Follower_starts_an_election_after_its_timeout()
    {
        var h = new NodeHarness();
        h.TickUntil(() => h.Node.Role == Role.Candidate);
        Assert.Equal(1, h.Node.Term);
        Assert.Equal(1, h.Node.VotedFor);
        var votes = h.Transport.Take<RequestVote>();
        Assert.Equal(new[] { 2, 3 }, votes.Select(v => v.To).OrderBy(x => x));
    }

    [Fact]
    public void Candidate_with_a_majority_becomes_leader_and_appends_a_noop()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        Assert.Equal(1, h.Node.LastLogIndex);
        var probes = h.Transport.Take<AppendEntries>();
        Assert.Equal(2, probes.Count);
        Assert.All(probes, p => Assert.True(p.Entries.Single().IsNoOp));
    }

    [Fact]
    public void Grants_at_most_one_vote_per_term()
    {
        var h = new NodeHarness();
        h.Deliver(new RequestVote(2, 1, 5, 0, 0));
        h.Deliver(new RequestVote(3, 1, 5, 0, 0));
        var replies = h.Transport.Take<RequestVoteReply>();
        Assert.True(replies[0].Granted);
        Assert.False(replies[1].Granted);
        Assert.Equal(2, h.Node.VotedFor);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Vote_requires_an_up_to_date_log_unless_the_bug_is_on(bool bug)
    {
        // Our log ends with an entry from term 3; the candidate's ends in term 2.
        var disk = NodeHarness.DiskWith(3, (1, "a"), (3, "b"));
        var h = new NodeHarness(bugs: bug ? InjectedBugs.VoteWithoutLogCheck : InjectedBugs.None, disk: disk);
        h.Deliver(new RequestVote(2, 1, 4, LastLogIndex: 5, LastLogTerm: 2));
        Assert.Equal(bug, h.Transport.Take<RequestVoteReply>().Single().Granted);
    }

    [Fact]
    public void Vote_reply_is_sent_only_after_term_and_vote_are_synced()
    {
        var h = new NodeHarness();
        h.Deliver(new RequestVote(2, 1, 1, 0, 0));
        Assert.Equal(new[] { "sync", "send RequestVoteReply" }, h.Log.Events);
        Assert.Equal(1, h.Disk.DurableTerm);
    }

    [Fact]
    public void Skip_fsync_bug_replies_with_nothing_on_disk()
    {
        var h = new NodeHarness(bugs: InjectedBugs.SkipFsync);
        h.Deliver(new RequestVote(2, 1, 1, 0, 0));
        Assert.Equal(new[] { "send RequestVoteReply" }, h.Log.Events);
        Assert.Equal(0, h.Disk.DurableTerm); // the vote was promised but is not on disk
        Assert.True(h.Disk.UnsyncedWrites > 0);
    }

    [Fact]
    public void Follower_acknowledges_appended_entries_only_after_sync()
    {
        var h = new NodeHarness();
        var entries = new[] { new LogEntry(1, 1, NodeHarness.Command("x")) };
        h.Deliver(new AppendEntries(2, 1, 1, 0, 0, entries, 0, 1));
        Assert.Equal(new[] { "sync", "send AppendEntriesReply" }, h.Log.Events);
        Assert.Equal(1, h.Disk.DurableLog.LastIndex);
    }

    [Fact]
    public void Follower_rejects_a_mismatched_previous_entry_with_a_hint()
    {
        var disk = NodeHarness.DiskWith(2, (1, "a"), (1, "b"), (1, "c"));
        var h = new NodeHarness(disk: disk);
        // The leader thinks index 3 has term 2; we have term 1 there, and term 1 starts at index 1.
        h.Deliver(new AppendEntries(2, 1, 2, 3, 2, Array.Empty<LogEntry>(), 0, 1));
        var reply = h.Transport.Take<AppendEntriesReply>().Single();
        Assert.False(reply.Success);
        Assert.Equal(1, reply.ConflictIndex);

        // Too far ahead: hint is our last index + 1.
        h.Deliver(new AppendEntries(2, 1, 2, 9, 2, Array.Empty<LogEntry>(), 0, 2));
        Assert.Equal(4, h.Transport.Take<AppendEntriesReply>().Single().ConflictIndex);
    }

    [Fact]
    public void Follower_truncates_a_conflicting_suffix_and_keeps_matching_entries()
    {
        var disk = NodeHarness.DiskWith(2, (1, "a"), (1, "b"), (2, "stale"));
        var h = new NodeHarness(disk: disk);
        var fresh = new[] { new LogEntry(1, 2, NodeHarness.Command("b")), new LogEntry(3, 3, NodeHarness.Command("new")) };
        // (1,2) matches what we already have, so it is kept as is; (3,3) conflicts with (2,3) and replaces it.
        var before = h.Node.Log.Get(2);
        h.Deliver(new AppendEntries(2, 1, 3, 1, 1, fresh, 0, 1));
        Assert.True(h.Transport.Take<AppendEntriesReply>().Single().Success);
        Assert.Same(before, h.Node.Log.Get(2));
        Assert.Equal(3, h.Node.Log.Get(3).Term);
        Assert.Equal(3, h.Disk.DurableLog.TermAt(3));
    }

    [Fact]
    public void A_reordered_old_append_does_not_truncate_newer_entries()
    {
        var h = new NodeHarness();
        var e1 = new LogEntry(1, 1, NodeHarness.Command("a"));
        var e2 = new LogEntry(1, 2, NodeHarness.Command("b"));
        h.Deliver(new AppendEntries(2, 1, 1, 0, 0, new[] { e1, e2 }, 0, 2));
        h.Deliver(new AppendEntries(2, 1, 1, 0, 0, new[] { e1 }, 0, 1)); // arrives late
        Assert.Equal(2, h.Node.LastLogIndex);
        var replies = h.Transport.Take<AppendEntriesReply>();
        Assert.Equal(new long[] { 2, 1 }, replies.Select(r => r.MatchIndex));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Leader_never_commits_an_earlier_term_entry_by_counting_replicas(bool bug)
    {
        // Index 1 holds an entry from term 1. We become leader of a later term and append a no-op at index 2.
        var disk = NodeHarness.DiskWith(1, (1, "old"));
        var h = new NodeHarness(bugs: bug ? InjectedBugs.CommitPreviousTerm : InjectedBugs.None, disk: disk);
        h.BecomeLeader();
        long term = h.Node.Term;
        Assert.Equal(2, h.Node.LastLogIndex);

        // Node 2 now stores index 1 but not yet the no-op: index 1 is on a majority (us + node 2).
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 1, 0, 0));
        Assert.Equal(bug ? 1 : 0, h.Node.CommitIndex);

        // Once the current-term no-op is on a majority, both commit (index 1 indirectly).
        h.Deliver(new AppendEntriesReply(2, 1, term, true, 2, 0, 0));
        Assert.Equal(2, h.Node.CommitIndex);
    }

    [Fact]
    public void Proposal_completes_when_a_majority_stores_it()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        ProposeResult? result = null;
        h.Node.Propose(new KvCommand(KvOp.Put, 9, 1, "k", "v", null).Encode(), r => result = r);
        h.Node.Flush();
        Assert.Null(result);
        h.Deliver(new AppendEntriesReply(3, 1, h.Node.Term, true, 2, 0, 0));
        Assert.Equal(ProposeStatus.Applied, result!.Value.Status);
        Assert.Equal("v", h.Sm.Get("k"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadIndex_waits_for_a_quorum_heartbeat_unless_the_bug_is_on(bool bug)
    {
        var h = new NodeHarness(bugs: bug ? InjectedBugs.StaleRead : InjectedBugs.None);
        h.BecomeLeader();
        h.Deliver(new AppendEntriesReply(2, 1, h.Node.Term, true, 1, 0, 0)); // no-op committed
        Assert.Equal(1, h.Node.CommitIndex);
        h.Transport.Sent.Clear();

        ReadStatus? status = null;
        h.Node.ReadIndex((s, _) => status = s);
        h.Node.Flush();
        if (bug)
        {
            Assert.Equal(ReadStatus.Ready, status);
            return;
        }
        Assert.Null(status);
        var heartbeats = h.Transport.Take<AppendEntries>();
        Assert.Equal(2, heartbeats.Count);
        long seq = heartbeats[0].Seq;

        // A reply to an older message proves nothing about leadership now.
        h.Deliver(new AppendEntriesReply(2, 1, h.Node.Term, true, 1, 0, seq - 1));
        Assert.Null(status);
        h.Deliver(new AppendEntriesReply(3, 1, h.Node.Term, true, 1, 0, seq));
        Assert.Equal(ReadStatus.Ready, status);
    }

    [Fact]
    public void Answer_to_a_stale_term_append_does_not_echo_its_sequence_number()
    {
        // Regression for chaos seed 18332: a leader that had restarted (its Seq counter back at 0) and was
        // now leader of this node's term counted the answer to its old message as a fresh ReadIndex
        // acknowledgement, and served a stale read while partitioned.
        var h = new NodeHarness();
        h.Deliver(new RequestVote(2, 1, 5, 0, 0));
        h.Transport.Sent.Clear();
        h.Deliver(new AppendEntries(2, 1, 3, 0, 0, Array.Empty<LogEntry>(), 0, Seq: 26));
        var reply = h.Transport.Take<AppendEntriesReply>().Single();
        Assert.Equal(5, reply.Term);
        Assert.False(reply.Success);
        Assert.Equal(0, reply.Seq);
        Assert.Equal(-1, reply.RejectedPrevIndex);
    }

    [Fact]
    public void Leader_steps_down_on_a_higher_term_and_fails_pending_work()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        ProposeResult? result = null;
        ReadStatus? read = null;
        h.Node.Propose(NodeHarness.Command("v"), r => result = r);
        h.Node.ReadIndex((s, _) => read = s);
        h.Node.Flush();
        h.Deliver(new AppendEntries(3, 1, h.Node.Term + 1, 0, 0, Array.Empty<LogEntry>(), 0, 1));
        Assert.Equal(Role.Follower, h.Node.Role);
        Assert.Equal(3, h.Node.LeaderId);
        Assert.Equal(ProposeStatus.Unknown, result!.Value.Status);
        Assert.Equal(ReadStatus.NotLeader, read);
    }

    [Fact]
    public void Leader_with_check_quorum_steps_down_when_isolated()
    {
        var h = new NodeHarness();
        h.BecomeLeader();
        h.TickUntil(() => h.Node.Role == Role.Follower, max: 50);
    }

    [Fact]
    public void InstallSnapshot_replaces_the_state_machine_and_log()
    {
        var disk = NodeHarness.DiskWith(1, (1, "a"), (1, "b"));
        var h = new NodeHarness(disk: disk);
        var sm = new KvStateMachine();
        sm.Apply(new KvCommand(KvOp.Put, 5, 1, "x", "from-snapshot", null));
        h.Deliver(new InstallSnapshot(2, 1, 2, 10, 2, sm.TakeSnapshot(), 1));
        Assert.Equal(10, h.Node.CommitIndex);
        Assert.Equal(10, h.Node.LastApplied);
        Assert.Equal(10, h.Node.SnapshotIndex);
        Assert.Equal(10, h.Node.LastLogIndex);
        Assert.Equal("from-snapshot", h.Sm.Get("x"));
        Assert.Equal(10, h.Transport.Take<InstallSnapshotReply>().Single().LastIndex);
    }

    [Fact]
    public void Node_takes_a_snapshot_after_the_threshold_and_restarts_from_it()
    {
        var disk = new SimDisk();
        var h = new NodeHarness(disk: disk, snapshotThreshold: 4);
        h.BecomeLeader();
        for (int i = 0; i < 6; i++)
        {
            h.Node.Propose(new KvCommand(KvOp.Put, 1, i + 1, $"k{i}", $"v{i}", null).Encode(), _ => { });
            h.Node.Flush();
            h.Deliver(new AppendEntriesReply(2, 1, h.Node.Term, true, h.Node.LastLogIndex, 0, 0));
        }
        Assert.Equal(7, h.Node.CommitIndex);
        Assert.True(h.Node.SnapshotIndex >= 4);

        var restarted = new NodeHarness(disk: disk, snapshotThreshold: 4);
        Assert.Equal(h.Node.Term, restarted.Node.Term);
        Assert.Equal(h.Node.SnapshotIndex, restarted.Node.SnapshotIndex);
        Assert.Equal(7, restarted.Node.LastLogIndex);
        Assert.Equal("v2", restarted.Sm.Get("k2")); // restored from the snapshot at index 4
        Assert.Null(restarted.Sm.Get("k3"));        // index 5: applied again only once the commit index is learned
    }

    [Fact]
    public void Restart_recovers_term_vote_and_log_from_disk()
    {
        var disk = new SimDisk();
        var h = new NodeHarness(disk: disk);
        h.Deliver(new RequestVote(2, 1, 4, 0, 0));
        h.Deliver(new AppendEntries(2, 1, 4, 0, 0, new[] { new LogEntry(4, 1, NodeHarness.Command("a")) }, 0, 1));
        var again = new NodeHarness(disk: disk);
        Assert.Equal(4, again.Node.Term);
        Assert.Equal(2, again.Node.VotedFor);
        Assert.Equal(1, again.Node.LastLogIndex);
    }
}
