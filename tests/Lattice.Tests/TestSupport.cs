using Lattice.Kv;
using Lattice.Raft;
using Lattice.Sim;
using Lattice.Util;

namespace Lattice.Tests;

/// <summary>A scratch directory under the test output folder, deleted afterwards.</summary>
public sealed class TempDir : IDisposable
{
    public TempDir()
    {
        Path = System.IO.Path.Combine(AppContext.BaseDirectory, "test-data", Guid.NewGuid().ToString("N")[..12]);
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, recursive: true); }
        catch (IOException) { /* a file may still be closing; leave it */ }
    }
}

/// <summary>Records the order of disk syncs and message sends, to test "fsync before reply".</summary>
internal sealed class EventLog
{
    public List<string> Events { get; } = new();
}

internal sealed class RecordingTransport(EventLog? log = null) : IRaftTransport
{
    public List<RaftMessage> Sent { get; } = new();

    public void Send(RaftMessage message)
    {
        Sent.Add(message);
        log?.Events.Add($"send {message.GetType().Name}");
    }

    public List<T> Take<T>() where T : RaftMessage
    {
        var result = Sent.OfType<T>().ToList();
        Sent.Clear();
        return result;
    }
}

internal sealed class RecordingStore(IDurableStore inner, EventLog log) : IDurableStore
{
    public PersistentState Load() => inner.Load();
    public void SetHardState(long term, int votedFor) => inner.SetHardState(term, votedFor);
    public void Append(LogEntry entry) => inner.Append(entry);
    public void TruncateFrom(long index) => inner.TruncateFrom(index);
    public void SaveSnapshot(Snapshot snapshot) => inner.SaveSnapshot(snapshot);

    public void Sync()
    {
        log.Events.Add("sync");
        inner.Sync();
    }
}

/// <summary>One RaftNode driven by hand: the test plays the role of the other nodes.</summary>
internal sealed class NodeHarness
{
    public NodeHarness(int clusterSize = 3, InjectedBugs bugs = InjectedBugs.None, SimDisk? disk = null, EventLog? log = null,
        int snapshotThreshold = 1000)
    {
        Disk = disk ?? new SimDisk();
        Log = log ?? new EventLog();
        Transport = new RecordingTransport(Log);
        Sm = new KvStateMachine();
        Options = new RaftOptions
        {
            ElectionTimeoutMinTicks = 10, ElectionTimeoutMaxTicks = 20, HeartbeatTicks = 2,
            SnapshotThreshold = snapshotThreshold, Bugs = bugs,
        };
        Node = new RaftNode(1, Enumerable.Range(1, clusterSize).ToArray(), Options, new RecordingStore(Disk, Log), Sm,
            Transport, new DetRandom(7));
    }

    public SimDisk Disk { get; }
    public EventLog Log { get; }
    public RecordingTransport Transport { get; }
    public KvStateMachine Sm { get; }
    public RaftOptions Options { get; }
    public RaftNode Node { get; }

    public void Deliver(RaftMessage m)
    {
        Node.Receive(m);
        Node.Flush();
    }

    public void TickUntil(Func<bool> condition, int max = 100)
    {
        for (int i = 0; i < max && !condition(); i++)
        {
            Node.Tick();
            Node.Flush();
        }
        Assert.True(condition(), "condition not reached by ticking");
    }

    /// <summary>Runs an election and wins it with a vote from node 2.</summary>
    public void BecomeLeader()
    {
        TickUntil(() => Node.Role == Role.Candidate);
        Transport.Sent.Clear();
        Deliver(new RequestVoteReply(2, 1, Node.Term, true));
        Assert.Equal(Role.Leader, Node.Role);
    }

    /// <summary>A SimDisk that already holds the given term and log, as if written by an earlier run.</summary>
    public static SimDisk DiskWith(long term, params (long Term, string Value)[] entries)
    {
        var disk = new SimDisk();
        disk.SetHardState(term, 0);
        long index = 1;
        foreach (var (t, v) in entries)
            disk.Append(new LogEntry(t, index++, Command(v)));
        disk.Sync();
        return disk;
    }

    public static byte[] Command(string value, long client = 1, long seq = 0) =>
        new KvCommand(KvOp.Put, client, seq == 0 ? 1 : seq, "k", value, null).Encode();
}
