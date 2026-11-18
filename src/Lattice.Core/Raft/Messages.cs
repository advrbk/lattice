namespace Lattice.Raft;

/// <summary>Base of every node-to-node message. Field names follow Figure 2 of the Raft paper.</summary>
public abstract record RaftMessage(int From, int To, long Term);

public sealed record RequestVote(int From, int To, long Term, long LastLogIndex, long LastLogTerm)
    : RaftMessage(From, To, Term);

public sealed record RequestVoteReply(int From, int To, long Term, bool Granted)
    : RaftMessage(From, To, Term);

/// <summary>
/// Replicates entries after (PrevLogIndex, PrevLogTerm). With no entries it is a heartbeat.
/// <see cref="Seq"/> is the leader's heartbeat round, echoed back so ReadIndex can tell which
/// replies were caused by messages sent after a read arrived.
/// </summary>
public sealed record AppendEntries(int From, int To, long Term, long PrevLogIndex, long PrevLogTerm,
    LogEntry[] Entries, long LeaderCommit, long Seq) : RaftMessage(From, To, Term);

/// <summary>
/// On success, <see cref="MatchIndex"/> is the last index known to match the leader. On failure,
/// <see cref="ConflictIndex"/> is where the leader should retry from (a hint, never trusted for safety)
/// and <see cref="RejectedPrevIndex"/> echoes the request's PrevLogIndex, so the leader can tell a reply
/// to its current probe from a stale or duplicated one.
/// </summary>
public sealed record AppendEntriesReply(int From, int To, long Term, bool Success, long MatchIndex,
    long ConflictIndex, long Seq, long RejectedPrevIndex = 0) : RaftMessage(From, To, Term);

/// <summary>Sends a whole snapshot in one message (no chunking).</summary>
public sealed record InstallSnapshot(int From, int To, long Term, long LastIndex, long LastTerm, byte[] Data, long Seq)
    : RaftMessage(From, To, Term);

public sealed record InstallSnapshotReply(int From, int To, long Term, long LastIndex, long Seq)
    : RaftMessage(From, To, Term);
