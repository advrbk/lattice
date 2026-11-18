namespace Lattice.Raft;

/// <summary>
/// The in-memory log: a snapshot covering indexes 1..SnapshotIndex followed by the entries after it.
/// Index arithmetic lives here so the node never has to think about the compaction offset.
/// </summary>
public sealed class RaftLog
{
    private readonly List<LogEntry> _entries = new();

    public RaftLog() : this(Snapshot.Empty) { }

    public RaftLog(Snapshot snapshot) => Snapshot = snapshot;

    public Snapshot Snapshot { get; private set; }
    public long SnapshotIndex => Snapshot.LastIndex;
    public long SnapshotTerm => Snapshot.LastTerm;
    public long FirstIndex => SnapshotIndex + 1;
    public long LastIndex => SnapshotIndex + _entries.Count;
    public long LastTerm => _entries.Count > 0 ? _entries[^1].Term : SnapshotTerm;
    public int Count => _entries.Count;
    public IReadOnlyList<LogEntry> Entries => _entries;

    /// <summary>The term of the entry at <paramref name="index"/>, or null if it was compacted away or does not exist.</summary>
    public long? TermAt(long index)
    {
        if (index == SnapshotIndex) return SnapshotTerm;
        if (index < SnapshotIndex || index > LastIndex) return null;
        return _entries[(int)(index - FirstIndex)].Term;
    }

    public LogEntry Get(long index)
    {
        if (index < FirstIndex || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), $"index {index} not in [{FirstIndex}, {LastIndex}]");
        return _entries[(int)(index - FirstIndex)];
    }

    public void Append(LogEntry entry)
    {
        if (entry.Index != LastIndex + 1)
            throw new InvalidOperationException($"append of index {entry.Index} after last index {LastIndex}");
        _entries.Add(entry);
    }

    /// <summary>Removes every entry with index &gt;= <paramref name="index"/>.</summary>
    public void TruncateFrom(long index)
    {
        if (index <= SnapshotIndex)
            throw new InvalidOperationException($"cannot truncate at {index}, snapshot covers up to {SnapshotIndex}");
        if (index > LastIndex) return;
        int start = (int)(index - FirstIndex);
        _entries.RemoveRange(start, _entries.Count - start);
    }

    /// <summary>Up to <paramref name="max"/> entries starting at <paramref name="from"/>.</summary>
    public LogEntry[] Slice(long from, int max)
    {
        if (from < FirstIndex) throw new ArgumentOutOfRangeException(nameof(from));
        if (from > LastIndex || max <= 0) return Array.Empty<LogEntry>();
        int start = (int)(from - FirstIndex);
        int n = (int)Math.Min(max, LastIndex - from + 1);
        var result = new LogEntry[n];
        _entries.CopyTo(start, result, 0, n);
        return result;
    }

    /// <summary>
    /// Replaces the prefix up to <c>snapshot.LastIndex</c> with the snapshot. If the log holds an entry
    /// at that index with the same term, the entries after it are kept (section 7 of the paper);
    /// otherwise the whole log is discarded.
    /// </summary>
    public void CompactTo(Snapshot snapshot)
    {
        if (snapshot.LastIndex <= SnapshotIndex) return;
        if (TermAt(snapshot.LastIndex) == snapshot.LastTerm)
            _entries.RemoveRange(0, (int)(snapshot.LastIndex - SnapshotIndex));
        else
            _entries.Clear();
        Snapshot = snapshot;
    }

    public RaftLog Clone()
    {
        var copy = new RaftLog(Snapshot);
        copy._entries.AddRange(_entries);
        return copy;
    }
}
