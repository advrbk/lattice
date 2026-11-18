namespace Lattice.Raft;

/// <summary>What a node finds on disk when it starts.</summary>
public sealed record PersistentState(long Term, int VotedFor, RaftLog Log);

/// <summary>
/// Durable storage for the three things Raft must not forget: current term, vote and log
/// (plus the snapshot that replaces the log prefix). Writes may be buffered; only
/// <see cref="Sync"/> makes them durable, and the node calls it before any reply that depends on them.
/// </summary>
public interface IDurableStore
{
    PersistentState Load();
    void SetHardState(long term, int votedFor);
    void Append(LogEntry entry);
    void TruncateFrom(long index);
    void SaveSnapshot(Snapshot snapshot);
    void Sync();
}

/// <summary>The application replicated by Raft. Apply must be deterministic.</summary>
public interface IStateMachine
{
    object? Apply(LogEntry entry);
    byte[] TakeSnapshot();
    void Restore(byte[] snapshot);
}

/// <summary>
/// The one interface both transports implement: the deterministic simulator's network and the TCP
/// transport. Delivery may be delayed, lost, duplicated or reordered; Raft does not care.
/// </summary>
public interface IRaftTransport
{
    void Send(RaftMessage message);
}

/// <summary>
/// Hooks the simulator uses to check Raft's safety properties as they happen. Production code passes null.
/// </summary>
public interface IRaftObserver
{
    void OnBecomeLeader(RaftNode node);
    void OnAppend(int nodeId, LogEntry entry, long prevTerm);
    void OnTruncate(int nodeId, long fromIndex, long commitIndex);
    /// <summary><paramref name="term"/> is the node's current term when it learned the entry was committed.</summary>
    void OnCommit(int nodeId, long term, LogEntry entry);
    void OnApply(int nodeId, LogEntry entry);
}
