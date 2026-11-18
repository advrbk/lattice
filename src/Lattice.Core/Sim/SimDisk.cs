using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Sim;

/// <summary>
/// A simulated disk for one node. Writes go into a volatile buffer (like the OS page cache) and only
/// become durable on <see cref="Sync"/> or when the simulated OS writes them back in the background.
/// <see cref="Crash"/> keeps a random prefix of the unsynced writes and loses the rest, so code that
/// replies before fsync can lose state it already promised to keep.
/// </summary>
public sealed class SimDisk : IDurableStore
{
    private abstract record Op;
    private sealed record HardStateOp(long Term, int Vote) : Op;
    private sealed record AppendOp(LogEntry Entry) : Op;
    private sealed record TruncateOp(long Index) : Op;
    private sealed record SnapshotOp(Snapshot Snapshot) : Op;

    private long _term;
    private int _vote;
    private RaftLog _log = new();
    private readonly List<Op> _unsynced = new();

    public int UnsyncedWrites => _unsynced.Count;
    public long SyncCount { get; private set; }
    public long DurableTerm => _term;
    public RaftLog DurableLog => _log;

    public PersistentState Load()
    {
        // A node restarting sees exactly what is durable, as a private copy.
        _unsynced.Clear();
        return new PersistentState(_term, _vote, _log.Clone());
    }

    public void SetHardState(long term, int votedFor) => _unsynced.Add(new HardStateOp(term, votedFor));
    public void Append(LogEntry entry) => _unsynced.Add(new AppendOp(entry));
    public void TruncateFrom(long index) => _unsynced.Add(new TruncateOp(index));
    public void SaveSnapshot(Snapshot snapshot) => _unsynced.Add(new SnapshotOp(snapshot));

    public void Sync()
    {
        SyncCount++;
        foreach (var op in _unsynced) ApplyDurably(op);
        _unsynced.Clear();
    }

    /// <summary>Power loss or process crash: an arbitrary prefix of the buffered writes made it out, the rest are gone.</summary>
    public void Crash(DetRandom rng)
    {
        int keep = _unsynced.Count == 0 ? 0 : rng.Next(_unsynced.Count + 1);
        for (int i = 0; i < keep; i++) ApplyDurably(_unsynced[i]);
        _unsynced.Clear();
    }

    private void ApplyDurably(Op op)
    {
        switch (op)
        {
            case HardStateOp h:
                _term = h.Term;
                _vote = h.Vote;
                break;
            case AppendOp a:
                if (a.Entry.Index <= _log.LastIndex) _log.TruncateFrom(a.Entry.Index);
                _log.Append(a.Entry);
                break;
            case TruncateOp t:
                if (t.Index > _log.SnapshotIndex) _log.TruncateFrom(t.Index);
                break;
            case SnapshotOp s:
                _log.CompactTo(s.Snapshot);
                break;
        }
    }
}
