using Lattice.Kv;
using Lattice.Raft;
using Lattice.Sim;
using Lattice.Util;

namespace Lattice.Tests;

/// <summary>
/// A cluster where the test decides exactly which messages are delivered, used to replay the
/// scenario of Figure 8 in the Raft paper step by step.
/// </summary>
internal sealed class ManualCluster
{
    private readonly int _n;
    private readonly RaftOptions _options;

    public ManualCluster(int n, RaftOptions options)
    {
        _n = n;
        _options = options;
        Disks = Enumerable.Range(0, n + 1).Select(_ => new SimDisk()).ToArray();
        Nodes = new RaftNode?[n + 1];
        Transports = new RecordingTransport[n + 1];
        for (int id = 1; id <= n; id++) Start(id);
    }

    public SimDisk[] Disks { get; }
    public RaftNode?[] Nodes { get; }
    public RecordingTransport[] Transports { get; }
    public RaftInvariants Invariants { get; } = new();

    public RaftNode this[int id] => Nodes[id]!;

    public void Start(int id)
    {
        Transports[id] = new RecordingTransport();
        Nodes[id] = new RaftNode(id, Enumerable.Range(1, _n).ToArray(), _options, Disks[id], new KvStateMachine(),
            Transports[id], new DetRandom((ulong)id * 1000 + (ulong)Disks[id].SyncCount), Invariants);
    }

    public void Crash(int id)
    {
        Nodes[id] = null;
        Transports[id].Sent.Clear(); // messages it had not sent yet are lost with it
        Disks[id].Crash(new DetRandom(1));
    }

    /// <summary>Ticks a node until it is a candidate in <paramref name="term"/>; vote requests from earlier terms are dropped.</summary>
    public void CampaignUntilTerm(int id, long term)
    {
        for (int i = 0; i < 500 && !(this[id].Role == Role.Candidate && this[id].Term == term); i++)
        {
            Transports[id].Sent.Clear();
            this[id].Tick();
            this[id].Flush();
        }
        Assert.Equal(Role.Candidate, this[id].Role);
        Assert.Equal(term, this[id].Term);
    }

    public void TickLeader(int id)
    {
        for (int i = 0; i < _options.HeartbeatTicks; i++)
        {
            this[id].Tick();
            this[id].Flush();
        }
    }

    /// <summary>
    /// Delivers messages between members of <paramref name="group"/> one at a time, in send order, until
    /// none are left or <paramref name="stop"/> holds. Messages rejected by <paramref name="filter"/> are lost;
    /// messages to nodes outside the group wait in the sender's outbox; messages to crashed nodes are lost.
    /// </summary>
    public void Exchange(int[] group, Func<RaftMessage, bool>? filter = null, Func<bool>? stop = null)
    {
        for (int guard = 0; guard < 10_000; guard++)
        {
            if (stop?.Invoke() == true) return;
            RaftMessage? next = null;
            foreach (int id in group)
            {
                var sent = Transports[id].Sent;
                sent.RemoveAll(m => Nodes[m.To] is null || (group.Contains(m.To) && filter is not null && !filter(m)));
                int k = sent.FindIndex(m => group.Contains(m.To));
                if (k >= 0)
                {
                    next = sent[k];
                    sent.RemoveAt(k);
                    break;
                }
            }
            if (next is null) return;
            this[next.To].Receive(next);
            this[next.To].Flush();
        }
        throw new InvalidOperationException("messages kept flowing");
    }

    public static bool VotesOnly(RaftMessage m) => m is RequestVote or RequestVoteReply;
}

public class Figure8Tests
{
    private static readonly byte[] X = new KvCommand(KvOp.Put, 1, 1, "k", "X", null).Encode();
    private static readonly byte[] Y = new KvCommand(KvOp.Put, 2, 1, "k", "Y", null).Encode();

    /// <summary>
    /// Figure 8 of the Raft paper, with Lattice's no-op entries included:
    /// (a) S1 leads term 1 and gets X (index 2) onto S2 only. (b) S1 crashes; S5 wins term 2 with S3 and
    /// S4 and appends at index 2 locally only. (c) S5 crashes; S1 restarts and wins term 3, then copies X
    /// to S3, so X sits on a majority (S1, S2, S3), but S1's own term-3 entry does not.
    /// (d) S1 crashes; S5 restarts and wins term 4 with S3 and S4, and will overwrite index 2.
    /// With the correct commit rule X was never committed in (c), so (d) is legal. With the injected bug S1
    /// commits X in (c) by counting replicas, and (d) then elects a leader without a committed entry.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Figure_8_committing_by_counting_replicas_loses_a_committed_entry(bool bug)
    {
        var options = new RaftOptions
        {
            ElectionTimeoutMinTicks = 10, ElectionTimeoutMaxTicks = 20, HeartbeatTicks = 2,
            MaxEntriesPerMessage = 1, CheckQuorum = false,
            Bugs = bug ? InjectedBugs.CommitPreviousTerm : InjectedBugs.None,
        };
        var c = new ManualCluster(5, options);
        int[] all = { 1, 2, 3, 4, 5 };

        // S1 becomes leader of term 1 and commits its no-op everywhere.
        c.CampaignUntilTerm(1, 1);
        c.Exchange(all);
        Assert.Equal(Role.Leader, c[1].Role);
        Assert.All(all, id => Assert.Equal(1, c[id].LastLogIndex));

        // (a) X reaches S2 only.
        c[1].Propose(X, _ => { });
        c[1].Flush();
        c.Exchange(new[] { 1, 2 });
        Assert.Equal(2, c[2].LastLogIndex);
        Assert.Equal(1, c[1].CommitIndex);
        c.Crash(1);

        // (b) S5 wins term 2 with votes from S3 and S4, appends locally, and crashes before sending anything.
        c.CampaignUntilTerm(5, 2);
        c.Exchange(new[] { 3, 4, 5 }, ManualCluster.VotesOnly);
        Assert.Equal(Role.Leader, c[5].Role);
        c[5].Propose(Y, _ => { });
        c[5].Flush();
        Assert.Equal(2, c[5].Log.TermAt(2));
        c.Crash(5);

        // (c) S1 restarts and wins term 3 with S2, S3 and S4.
        c.Start(1);
        c.CampaignUntilTerm(1, 3);
        c.Exchange(new[] { 1, 2, 3, 4 }, ManualCluster.VotesOnly);
        Assert.Equal(Role.Leader, c[1].Role);
        c.TickLeader(1);
        // S2 already has X, so it takes the term-3 no-op straight away.
        c.Exchange(new[] { 1, 2 });
        Assert.Equal(3, c[2].Log.TermAt(3));
        // S3 lacks X. With one entry per message, X reaches S3 before the no-op does. Stop as soon as S1
        // has processed S3's acknowledgement of X; the no-op S1 sends next is lost when S1 crashes.
        c.Exchange(new[] { 1, 3 }, stop: () => c[3].LastLogIndex == 2 && c.Transports[3].Sent.Count == 0);
        Assert.Equal(1, c[3].Log.TermAt(2));
        // The restarted S1 starts with commit index 0 (it is not persisted). The correct rule cannot commit
        // anything until the term-3 no-op is on a majority; the buggy rule commits X right here.
        Assert.Equal(bug ? 2 : 0, c[1].CommitIndex);
        c.Crash(1);

        // (d) S5 restarts and runs for term 4. S3 and S4 vote for it: its last log term (2) beats theirs (1).
        c.Start(5);
        c.CampaignUntilTerm(5, 4);
        if (bug)
        {
            var violation = Assert.Throws<InvariantViolation>(() => c.Exchange(new[] { 3, 4, 5 }, ManualCluster.VotesOnly));
            Assert.Equal("LeaderCompleteness", violation.Invariant);
            return;
        }

        c.Exchange(new[] { 3, 4, 5 }, ManualCluster.VotesOnly);
        Assert.Equal(Role.Leader, c[5].Role);
        // Legal: X was never committed, so S5 replaces it everywhere, and all logs converge.
        c.Start(1);
        for (int round = 0; round < 20; round++)
        {
            c.TickLeader(5);
            c.Exchange(all);
        }
        foreach (int id in all)
        {
            Assert.Equal(c[5].LastLogIndex, c[id].LastLogIndex);
            Assert.Equal(2, c[id].Log.TermAt(2));
            Assert.Equal(c[5].LastLogIndex, c[id].CommitIndex);
        }
    }
}
