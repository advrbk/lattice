namespace Lattice.Raft;

/// <summary>
/// Deliberate bugs that can be switched on to show that the chaos test and the linearizability
/// checker catch them. All are off in normal use.
/// </summary>
[Flags]
public enum InjectedBugs
{
    None = 0,
    /// <summary>Grant votes without the "candidate log is at least as up-to-date" check (section 5.4.1).</summary>
    VoteWithoutLogCheck = 1,
    /// <summary>Commit entries from earlier terms by counting replicas (the Figure 8 mistake, section 5.4.2).</summary>
    CommitPreviousTerm = 2,
    /// <summary>Reply to votes and appends without fsyncing term, vote and log first.</summary>
    SkipFsync = 4,
    /// <summary>Serve ReadIndex reads without confirming leadership with a quorum.</summary>
    StaleRead = 8,
    /// <summary>Apply every command even when its (client, sequence) pair was already applied.</summary>
    NoDedupe = 16,
}

public sealed class RaftOptions
{
    public int ElectionTimeoutMinTicks { get; init; } = 15;
    public int ElectionTimeoutMaxTicks { get; init; } = 30;
    public int HeartbeatTicks { get; init; } = 3;
    public int MaxEntriesPerMessage { get; init; } = 64;
    public int MaxInflightEntries { get; init; } = 1024;
    /// <summary>Take a snapshot once this many applied entries sit in the log after the last snapshot.</summary>
    public int SnapshotThreshold { get; init; } = 1000;
    /// <summary>A leader that has not heard from a majority for an election timeout steps down.</summary>
    public bool CheckQuorum { get; init; } = true;
    public InjectedBugs Bugs { get; init; }

    public bool Has(InjectedBugs bug) => (Bugs & bug) != 0;
}
