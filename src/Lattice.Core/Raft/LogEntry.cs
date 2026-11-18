namespace Lattice.Raft;

/// <summary>One slot of the replicated log. An entry with empty <see cref="Data"/> is a no-op.</summary>
public sealed record LogEntry(long Term, long Index, byte[] Data)
{
    public bool IsNoOp => Data.Length == 0;
}

/// <summary>A compacted prefix of the log: the state machine as of <see cref="LastIndex"/>.</summary>
public sealed record Snapshot(long LastIndex, long LastTerm, byte[] Data)
{
    public static readonly Snapshot Empty = new(0, 0, Array.Empty<byte>());
}
