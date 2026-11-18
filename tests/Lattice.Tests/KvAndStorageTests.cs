using Lattice.Kv;
using Lattice.Net;
using Lattice.Raft;
using Lattice.Sim;
using Lattice.Storage;
using Lattice.Util;

namespace Lattice.Tests;

public class KvStateMachineTests
{
    private static KvCommand Cmd(KvOp op, long client, long seq, string key, string? value = null, string? expected = null) =>
        new(op, client, seq, key, value, expected);

    [Fact]
    public void Put_get_delete_and_cas()
    {
        var sm = new KvStateMachine();
        sm.Apply(Cmd(KvOp.Put, 1, 1, "a", "1"));
        Assert.Equal("1", sm.Apply(Cmd(KvOp.Get, 1, 2, "a")).Value);
        Assert.False(sm.Apply(Cmd(KvOp.Cas, 1, 3, "a", "2", "wrong")).Success);
        var swap = sm.Apply(Cmd(KvOp.Cas, 1, 4, "a", "2", "1"));
        Assert.True(swap.Success);
        Assert.Equal("1", swap.Value);
        Assert.Equal("2", sm.Get("a"));
        Assert.True(sm.Apply(Cmd(KvOp.Delete, 1, 5, "a")).Success);
        Assert.Null(sm.Get("a"));
        Assert.True(sm.Apply(Cmd(KvOp.Cas, 1, 6, "a", "x", null)).Success); // null expected = key must be absent
        Assert.Equal("x", sm.Get("a"));
    }

    [Fact]
    public void Retried_command_is_applied_once_and_returns_the_first_result()
    {
        var sm = new KvStateMachine();
        sm.Apply(Cmd(KvOp.Put, 1, 1, "a", "0"));
        var first = sm.Apply(Cmd(KvOp.Cas, 2, 1, "a", "1", "0"));
        sm.Apply(Cmd(KvOp.Put, 3, 1, "a", "0")); // someone sets it back
        var retry = sm.Apply(Cmd(KvOp.Cas, 2, 1, "a", "1", "0"));
        Assert.True(first.Success);
        Assert.True(retry.Success); // the cached answer, not a second swap
        Assert.Equal("0", sm.Get("a"));
    }

    [Fact]
    public void Without_sessions_a_retry_applies_twice()
    {
        var sm = new KvStateMachine(dedupe: false);
        sm.Apply(Cmd(KvOp.Put, 1, 1, "a", "0"));
        Assert.True(sm.Apply(Cmd(KvOp.Cas, 2, 1, "a", "1", "0")).Success);
        Assert.False(sm.Apply(Cmd(KvOp.Cas, 2, 1, "a", "1", "0")).Success); // the client now hears "failed"
    }

    [Fact]
    public void Snapshot_round_trip_keeps_data_and_sessions()
    {
        var sm = new KvStateMachine();
        sm.Apply(Cmd(KvOp.Put, 1, 1, "b", "2"));
        sm.Apply(Cmd(KvOp.Put, 2, 7, "a", "1"));
        var copy = new KvStateMachine();
        copy.Restore(sm.TakeSnapshot());
        Assert.Equal(sm.Digest(), copy.Digest());
        Assert.Equal("1", copy.Get("a"));
        copy.Apply(Cmd(KvOp.Put, 2, 7, "a", "changed")); // seq 7 already applied for client 2
        Assert.Equal("1", copy.Get("a"));
    }

    [Fact]
    public void Command_encoding_round_trips()
    {
        var c = Cmd(KvOp.Cas, long.MaxValue, 5, "ключ", null, "é");
        Assert.Equal(c, KvCommand.Decode(c.Encode()));
    }
}

public class StorageTests
{
    private static LogEntry E(long term, long index, string data = "x") => new(term, index, System.Text.Encoding.UTF8.GetBytes(data));

    [Fact]
    public void RaftLog_index_arithmetic_and_compaction()
    {
        var log = new RaftLog();
        for (int i = 1; i <= 5; i++) log.Append(E(i < 4 ? 1 : 2, i));
        Assert.Equal(5, log.LastIndex);
        Assert.Equal(2, log.LastTerm);
        Assert.Equal(new long[] { 3, 4 }, log.Slice(3, 2).Select(e => e.Index));
        log.CompactTo(new Snapshot(3, 1, Array.Empty<byte>()));
        Assert.Equal(4, log.FirstIndex);
        Assert.Equal(1, log.TermAt(3));
        Assert.Null(log.TermAt(2));
        Assert.Throws<InvalidOperationException>(() => log.TruncateFrom(3));
        log.TruncateFrom(5);
        Assert.Equal(4, log.LastIndex);
        Assert.Throws<InvalidOperationException>(() => log.Append(E(2, 7)));
        log.CompactTo(new Snapshot(10, 3, Array.Empty<byte>())); // unknown index: whole log discarded
        Assert.Equal(10, log.LastIndex);
        Assert.Equal(0, log.Count);
    }

    [Fact]
    public void SimDisk_loses_unsynced_writes_on_crash()
    {
        var disk = new SimDisk();
        disk.SetHardState(1, 2);
        disk.Append(E(1, 1));
        disk.Sync();
        for (int i = 2; i <= 50; i++) disk.Append(E(1, i));
        disk.SetHardState(9, 3);
        disk.Crash(new DetRandom(3));
        var state = disk.Load();
        Assert.InRange(state.Log.LastIndex, 1, 50); // some prefix of the unsynced writes survived
        Assert.Contains(state.Term, new long[] { 1, 9 });
        if (state.Term == 9) Assert.Equal(50, state.Log.LastIndex); // the term was written last, so everything before it survived too
        Assert.Equal(1, state.Log.Get(1).Term);
    }

    [Fact]
    public void File_store_survives_restart_with_truncation_and_snapshot()
    {
        using var dir = new TempDir();
        using (var store = new FileDurableStore(dir.Path))
        {
            store.Load();
            store.SetHardState(3, 2);
            for (int i = 1; i <= 6; i++) store.Append(E(1, i, $"v{i}"));
            store.TruncateFrom(5);
            store.Append(E(3, 5, "new"));
            store.Sync();
        }
        using (var store = new FileDurableStore(dir.Path))
        {
            var s = store.Load();
            Assert.Equal(3, s.Term);
            Assert.Equal(2, s.VotedFor);
            Assert.Equal(5, s.Log.LastIndex);
            Assert.Equal(3, s.Log.TermAt(5));
            store.SaveSnapshot(new Snapshot(3, 1, new byte[] { 1, 2, 3 }));
            store.Append(E(3, 6, "after"));
            store.Sync();
        }
        using (var store = new FileDurableStore(dir.Path))
        {
            var s = store.Load();
            Assert.Equal(3, s.Log.SnapshotIndex);
            Assert.Equal(new byte[] { 1, 2, 3 }, s.Log.Snapshot.Data);
            Assert.Equal(new long[] { 4, 5, 6 }, s.Log.Entries.Select(e => e.Index));
            Assert.Equal("after", System.Text.Encoding.UTF8.GetString(s.Log.Get(6).Data));
        }
    }

    [Fact]
    public void File_store_drops_a_torn_tail_and_keeps_appending()
    {
        using var dir = new TempDir();
        using (var store = new FileDurableStore(dir.Path))
        {
            store.Load();
            for (int i = 1; i <= 3; i++) store.Append(E(1, i));
            store.Sync();
        }
        string log = Path.Combine(dir.Path, "log");
        var bytes = File.ReadAllBytes(log);
        File.WriteAllBytes(log, bytes[..^5]); // the last record was cut off mid-write
        using (var store = new FileDurableStore(dir.Path))
        {
            var s = store.Load();
            Assert.Equal(2, s.Log.LastIndex);
            store.Append(E(2, 3, "rewritten"));
            store.Sync();
        }
        using (var store = new FileDurableStore(dir.Path))
        {
            var s = store.Load();
            Assert.Equal(3, s.Log.LastIndex);
            Assert.Equal(2, s.Log.TermAt(3));
        }
    }

    [Fact]
    public void File_store_recovers_from_a_crash_between_snapshot_and_log_rewrite()
    {
        using var dir = new TempDir();
        using (var store = new FileDurableStore(dir.Path))
        {
            store.Load();
            for (int i = 1; i <= 5; i++) store.Append(E(1, i));
            store.Sync();
        }
        // Simulate: the new snapshot file was written, but the old log (base 0) was never rewritten.
        string logCopy = File.ReadAllText(Path.Combine(dir.Path, "log"), System.Text.Encoding.Latin1);
        using (var store = new FileDurableStore(dir.Path))
        {
            store.Load();
            store.SaveSnapshot(new Snapshot(3, 1, new byte[] { 9 }));
        }
        File.WriteAllText(Path.Combine(dir.Path, "log"), logCopy, System.Text.Encoding.Latin1);
        using (var store = new FileDurableStore(dir.Path))
        {
            var s = store.Load();
            Assert.Equal(3, s.Log.SnapshotIndex);
            Assert.Equal(new long[] { 4, 5 }, s.Log.Entries.Select(e => e.Index));
        }
    }

    [Fact]
    public void File_store_rejects_a_corrupted_hard_state()
    {
        using var dir = new TempDir();
        using (var store = new FileDurableStore(dir.Path))
        {
            store.Load();
            store.SetHardState(5, 1);
            store.Sync();
        }
        var path = Path.Combine(dir.Path, "hardstate");
        var b = File.ReadAllBytes(path);
        b[0] ^= 0xFF;
        File.WriteAllBytes(path, b);
        using var again = new FileDurableStore(dir.Path);
        Assert.Throws<InvalidDataException>(() => again.Load());
    }
}

public class WireTests
{
    public static IEnumerable<object[]> Messages()
    {
        var entries = new[] { new LogEntry(2, 7, new byte[] { 1, 2 }), new LogEntry(2, 8, Array.Empty<byte>()) };
        yield return new object[] { new RequestVote(1, 2, 3, 4, 5) };
        yield return new object[] { new RequestVoteReply(2, 1, 3, true) };
        yield return new object[] { new AppendEntries(1, 3, 4, 6, 2, entries, 5, 99) };
        yield return new object[] { new AppendEntriesReply(3, 1, 4, false, 0, 6, 99, 5) };
        yield return new object[] { new InstallSnapshot(1, 2, 4, 100, 3, new byte[] { 9, 9 }, 12) };
        yield return new object[] { new InstallSnapshotReply(2, 1, 4, 100, 12) };
        yield return new object[] { new ClientRequest(77, 3, KvOp.Cas, "k", null, "old") };
        yield return new object[] { new ClientResponse(3, ClientStatus.NotLeader, false, null, 2) };
        yield return new object[] { new StatusResponse(1, Role.Leader, 9, 1, 50, 50, 51, 40, 7) };
    }

    [Theory]
    [MemberData(nameof(Messages))]
    public void Every_message_round_trips(object message)
    {
        var frame = Wire.Encode(message);
        Assert.Equal(frame.Length - 4, BitConverter.ToInt32(frame, 0));
        var decoded = Wire.Decode(frame[4..]);
        if (message is AppendEntries ae)
        {
            var d = (AppendEntries)decoded;
            Assert.Equal(ae with { Entries = Array.Empty<LogEntry>() }, d with { Entries = Array.Empty<LogEntry>() });
            Assert.Equal(ae.Entries.Select(e => (e.Term, e.Index, Convert.ToHexString(e.Data))),
                d.Entries.Select(e => (e.Term, e.Index, Convert.ToHexString(e.Data))));
        }
        else if (message is InstallSnapshot snap)
        {
            var d = (InstallSnapshot)decoded;
            Assert.Equal(snap.Data, d.Data);
            Assert.Equal(snap with { Data = Array.Empty<byte>() }, d with { Data = Array.Empty<byte>() });
        }
        else
        {
            Assert.Equal(message, decoded);
        }
    }

    [Fact]
    public void DetRandom_is_reproducible()
    {
        var a = new DetRandom(123);
        var b = new DetRandom(123);
        Assert.Equal(Enumerable.Range(0, 100).Select(_ => a.NextU64()), Enumerable.Range(0, 100).Select(_ => b.NextU64()));
        // Pinned values: the same seed must produce the same run on any machine and .NET version.
        var c = new DetRandom(1);
        Assert.Equal(new[] { c.Next(1000), c.Next(1000), c.Next(1000) }, new[] { Pinned[0], Pinned[1], Pinned[2] });
    }

    internal static readonly int[] Pinned = { 557, 522, 900 };
}
