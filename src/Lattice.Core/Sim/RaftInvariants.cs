using Lattice.Raft;

namespace Lattice.Sim;

public sealed class InvariantViolation : Exception
{
    public InvariantViolation(string invariant, string message) : base($"{invariant}: {message}") => Invariant = invariant;

    public string Invariant { get; }
}

/// <summary>
/// Checks the safety properties of Figure 3 of the Raft paper while the simulation runs, from the
/// observer hooks every node calls. A violation throws immediately, so the failure points at the
/// event that broke it rather than at a symptom much later.
/// <list type="bullet">
/// <item>Election safety: at most one leader per term.</item>
/// <item>Log matching: two entries with the same index and term have the same command and the same
/// preceding term, which by induction means identical logs up to that index.</item>
/// <item>Leader completeness: a new leader of term T holds every entry committed in a term before T.
/// (A leader of an older term can still be elected late, for example after a long pause; it does not
/// need entries committed in later terms, and it cannot commit anything because its term is stale.)</item>
/// <item>State machine safety: no two nodes commit or apply different entries at the same index.</item>
/// </list>
/// Also: no node ever truncates an entry at or below its own commit index.
/// </summary>
public sealed class RaftInvariants : IRaftObserver
{
    private readonly bool _enforce;
    private readonly Dictionary<long, int> _leaderOfTerm = new();
    private readonly Dictionary<(long Index, long Term), (byte[] Data, long PrevTerm)> _entries = new();
    private readonly Dictionary<long, (LogEntry Entry, long CommitTerm)> _committed = new();
    private long _maxCommitted;

    /// <param name="enforce">False keeps the statistics but never reports a violation (to test the linearizability checker alone).</param>
    public RaftInvariants(bool enforce = true) => _enforce = enforce;

    public int Elections => _leaderOfTerm.Count;

    private void Fail(string invariant, string message)
    {
        if (_enforce) throw new InvariantViolation(invariant, message);
    }
    public long MaxCommitted => _maxCommitted;

    public void OnBecomeLeader(RaftNode node)
    {
        if (_leaderOfTerm.TryGetValue(node.Term, out int other) && other != node.Id)
            Fail("ElectionSafety", $"nodes {other} and {node.Id} were both elected leader of term {node.Term}");
        _leaderOfTerm[node.Term] = node.Id;

        var log = node.Log;
        for (long i = log.SnapshotIndex + 1; i <= _maxCommitted; i++)
        {
            if (!_committed.TryGetValue(i, out var cc) || cc.CommitTerm >= node.Term) continue;
            var c = cc.Entry;
            long? have = log.TermAt(i);
            if (have != c.Term)
                Fail("LeaderCompleteness",
                    $"node {node.Id} became leader of term {node.Term} without committed entry {i} (term {c.Term}); its log has {(have is null ? "nothing" : $"term {have}")} there");
        }
    }

    public void OnAppend(int nodeId, LogEntry entry, long prevTerm)
    {
        var key = (entry.Index, entry.Term);
        if (_entries.TryGetValue(key, out var seen))
        {
            if (!SameBytes(seen.Data, entry.Data) || seen.PrevTerm != prevTerm)
                Fail("LogMatching",
                    $"node {nodeId} has a different entry at index {entry.Index} term {entry.Term} (or a different predecessor term: {prevTerm} vs {seen.PrevTerm})");
        }
        else
        {
            _entries[key] = (entry.Data, prevTerm);
        }
    }

    public void OnTruncate(int nodeId, long fromIndex, long commitIndex)
    {
        if (fromIndex <= commitIndex)
            Fail("CommittedEntryRemoved",
                $"node {nodeId} truncated its log from index {fromIndex} but had committed up to {commitIndex}");
    }

    public void OnCommit(int nodeId, long term, LogEntry entry)
    {
        if (_committed.TryGetValue(entry.Index, out var cc))
        {
            var c = cc.Entry;
            if (c.Term != entry.Term || !SameBytes(c.Data, entry.Data))
                Fail("StateMachineSafety",
                    $"node {nodeId} committed term {entry.Term} at index {entry.Index}, but term {c.Term} was already committed there");
            else if (term < cc.CommitTerm)
                _committed[entry.Index] = (c, term); // keep the earliest term in which it was known committed
        }
        else
        {
            _committed[entry.Index] = (entry, term);
            if (entry.Index > _maxCommitted) _maxCommitted = entry.Index;
        }
    }

    public void OnApply(int nodeId, LogEntry entry)
    {
        if (!_committed.TryGetValue(entry.Index, out var cc) || cc.Entry.Term != entry.Term || !SameBytes(cc.Entry.Data, entry.Data))
            Fail("StateMachineSafety",
                $"node {nodeId} applied term {entry.Term} at index {entry.Index}, which is not the committed entry there");
    }

    private static bool SameBytes(byte[] a, byte[] b) => ReferenceEquals(a, b) || a.AsSpan().SequenceEqual(b);
}
