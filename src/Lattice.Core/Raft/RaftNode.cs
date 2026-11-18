using Lattice.Util;

namespace Lattice.Raft;

public enum Role { Follower, Candidate, Leader }

public enum ProposeStatus
{
    /// <summary>The entry was committed and applied; Value is the state machine's result.</summary>
    Applied,
    /// <summary>This node is not the leader; nothing was appended. LeaderHint may name the leader.</summary>
    NotLeader,
    /// <summary>Leadership was lost after the entry was appended. It may or may not commit later.</summary>
    Unknown,
}

public readonly record struct ProposeResult(ProposeStatus Status, object? Value, int LeaderHint);

public enum ReadStatus
{
    /// <summary>The local state machine is now at least as new as every write that completed before the read began.</summary>
    Ready,
    NotLeader,
}

/// <summary>
/// One Raft server, written as a single-threaded, event-driven state machine. The host calls
/// <see cref="Tick"/>, <see cref="Receive"/>, <see cref="Propose"/> and <see cref="ReadIndex"/>, then
/// <see cref="Flush"/>. Handlers write to the durable store and queue outgoing messages; Flush fsyncs the
/// store and only then sends them, so nothing leaves the node that depends on state not yet on disk.
/// The node has no threads, clocks or sockets of its own, which is what lets the same code run inside
/// the deterministic simulator and behind the TCP transport.
/// </summary>
public sealed class RaftNode
{
    private enum PeerState { Probe, Replicate, Snapshot }

    private sealed class Peer
    {
        public required int Id;
        public long Match;
        public long Next;
        public PeerState State;
        public bool ProbeSent;
        public long AckSeq;
        public bool RecentActive;
        public long SnapshotSentAt;
    }

    private sealed class PendingRead
    {
        public required long ReadIndex;
        public required long Seq;
        public required Action<ReadStatus, int> Callback;
        public bool Confirmed;
    }

    private readonly RaftOptions _opt;
    private readonly IDurableStore _store;
    private readonly IStateMachine _sm;
    private readonly IRaftTransport _transport;
    private readonly IRaftObserver? _observer;
    private readonly DetRandom _rng;
    private readonly Peer[] _peers;
    private readonly int _clusterSize;
    private readonly RaftLog _log;
    private readonly List<RaftMessage> _outbox = new();
    private readonly Dictionary<long, (long Term, Action<ProposeResult> Callback)> _pending = new();
    private readonly List<PendingRead> _reads = new();
    private readonly HashSet<int> _votes = new();

    private int _votedFor;
    private int _electionElapsed;
    private int _heartbeatElapsed;
    private int _randomizedElectionTimeout;
    private bool _dirty;
    private bool _needReplicate;
    private bool _readBroadcast;
    private long _selfMatch;
    private long _heartbeatSeq;
    private long _termStartIndex;
    private long _ticks;

    public RaftNode(int id, IReadOnlyList<int> clusterIds, RaftOptions options, IDurableStore store,
        IStateMachine stateMachine, IRaftTransport transport, DetRandom random, IRaftObserver? observer = null)
    {
        if (!clusterIds.Contains(id)) throw new ArgumentException("node id must be part of the cluster");
        if (id <= 0) throw new ArgumentException("node ids start at 1 (0 means 'nobody')");
        Id = id;
        _opt = options;
        _store = store;
        _sm = stateMachine;
        _transport = transport;
        _observer = observer;
        _rng = random;
        _clusterSize = clusterIds.Count;
        _peers = clusterIds.Where(p => p != id).Select(p => new Peer { Id = p }).ToArray();

        var state = store.Load();
        Term = state.Term;
        _votedFor = state.VotedFor;
        _log = state.Log;
        if (_log.SnapshotIndex > 0) _sm.Restore(_log.Snapshot.Data);
        CommitIndex = LastApplied = _log.SnapshotIndex;
        Role = Role.Follower;
        ResetElectionTimer();
    }

    public int Id { get; }
    public Role Role { get; private set; }
    public long Term { get; private set; }
    public int LeaderId { get; private set; }
    public int VotedFor => _votedFor;
    public long CommitIndex { get; private set; }
    public long LastApplied { get; private set; }
    public long LastLogIndex => _log.LastIndex;
    public long LastLogTerm => _log.LastTerm;
    public long SnapshotIndex => _log.SnapshotIndex;
    public int PendingProposals => _pending.Count;
    internal RaftLog Log => _log;

    private int Majority => _clusterSize / 2 + 1;
    private bool Has(InjectedBugs bug) => _opt.Has(bug);

    // ------------------------------------------------------------------ inputs

    /// <summary>Advances the logical clock by one tick (heartbeats, election timeouts, check-quorum).</summary>
    public void Tick()
    {
        _ticks++;
        _electionElapsed++;
        if (Role == Role.Leader)
        {
            if (++_heartbeatElapsed >= _opt.HeartbeatTicks)
            {
                _heartbeatElapsed = 0;
                foreach (var p in _peers) SendTo(p, heartbeat: true);
            }
            if (_opt.CheckQuorum && _electionElapsed >= _opt.ElectionTimeoutMinTicks)
            {
                _electionElapsed = 0;
                int active = 1;
                foreach (var p in _peers)
                {
                    if (p.RecentActive) active++;
                    p.RecentActive = false;
                }
                if (active < Majority) BecomeFollower(Term, 0);
            }
        }
        else if (_electionElapsed >= _randomizedElectionTimeout)
        {
            StartElection();
        }
    }

    public void Receive(RaftMessage m)
    {
        if (m.To != Id) return;

        if (m.Term > Term)
        {
            int leader = m is AppendEntries or InstallSnapshot ? m.From : 0;
            BecomeFollower(m.Term, leader);
        }
        else if (m.Term < Term)
        {
            // A stale sender. Answer requests with our term so it steps down; drop stale replies.
            // These answers must not echo the request's Seq: by the time one arrives the sender may have
            // restarted (its Seq counter starts again at 0) and become leader of exactly our term, and it
            // would then count an answer to its old message as a fresh acknowledgement for ReadIndex.
            // Seq 0 never confirms a read, and RejectedPrevIndex -1 never matches an outstanding probe.
            switch (m)
            {
                case RequestVote rv:
                    Send(new RequestVoteReply(Id, rv.From, Term, false));
                    break;
                case AppendEntries ae:
                    Send(new AppendEntriesReply(Id, ae.From, Term, false, 0, 0, 0, RejectedPrevIndex: -1));
                    break;
                case InstallSnapshot snap:
                    Send(new InstallSnapshotReply(Id, snap.From, Term, -1, 0));
                    break;
            }
            return;
        }

        switch (m)
        {
            case RequestVote rv: HandleRequestVote(rv); break;
            case RequestVoteReply rvr: HandleRequestVoteReply(rvr); break;
            case AppendEntries ae: HandleAppendEntries(ae); break;
            case AppendEntriesReply aer: HandleAppendEntriesReply(aer); break;
            case InstallSnapshot snap: HandleInstallSnapshot(snap); break;
            case InstallSnapshotReply sr: HandleInstallSnapshotReply(sr); break;
        }
    }

    /// <summary>
    /// Appends a command if this node is the leader. The callback runs once: when the entry is applied,
    /// when leadership is lost, or immediately if this node is not the leader.
    /// </summary>
    public void Propose(byte[] data, Action<ProposeResult> callback)
    {
        if (data.Length == 0) throw new ArgumentException("empty commands are reserved for no-ops");
        if (Role != Role.Leader)
        {
            callback(new ProposeResult(ProposeStatus.NotLeader, null, LeaderId));
            return;
        }
        var entry = new LogEntry(Term, _log.LastIndex + 1, data);
        AppendLocal(entry);
        _pending[entry.Index] = (Term, callback);
        _needReplicate = true;
    }

    /// <summary>
    /// Linearizable read barrier (ReadIndex, section 6.4 of Ongaro's dissertation). The read index is the
    /// commit index, but at least this term's no-op, so every write that completed before the call is
    /// included. The leader then confirms it is still leader with a round of heartbeats acknowledged by a
    /// majority, and the callback runs once the state machine has applied up to the read index.
    /// </summary>
    public void ReadIndex(Action<ReadStatus, int> callback)
    {
        if (Role != Role.Leader)
        {
            callback(ReadStatus.NotLeader, LeaderId);
            return;
        }
        var read = new PendingRead
        {
            ReadIndex = Math.Max(CommitIndex, _termStartIndex),
            Seq = _heartbeatSeq + 1,
            Callback = callback,
            Confirmed = _peers.Length == 0 || Has(InjectedBugs.StaleRead),
        };
        _reads.Add(read);
        if (!read.Confirmed) _readBroadcast = true;
    }

    /// <summary>
    /// Makes buffered writes durable, then advances the commit index, applies entries, completes reads
    /// and finally sends the queued messages. Hosts call it after each batch of inputs.
    /// </summary>
    public void Flush()
    {
        if (_dirty)
        {
            if (!Has(InjectedBugs.SkipFsync)) _store.Sync();
            _dirty = false;
        }

        if (Role == Role.Leader)
        {
            // The leader counts itself towards a majority only for entries that are on its own disk.
            _selfMatch = _log.LastIndex;
            MaybeAdvanceCommit();
            if (_needReplicate)
            {
                _needReplicate = false;
                foreach (var p in _peers) SendTo(p, heartbeat: false);
            }
            if (_readBroadcast)
            {
                _readBroadcast = false;
                _heartbeatSeq++;
                foreach (var p in _peers)
                    if (p.State != PeerState.Snapshot) SendTo(p, heartbeat: true);
            }
            ConfirmReads();
        }

        ApplyCommitted();
        CompleteReads();

        if (_outbox.Count > 0)
        {
            var batch = _outbox.ToArray();
            _outbox.Clear();
            foreach (var m in batch) _transport.Send(m);
        }
    }

    // ------------------------------------------------------------------ elections

    private void ResetElectionTimer()
    {
        _electionElapsed = 0;
        _randomizedElectionTimeout = _rng.Next(_opt.ElectionTimeoutMinTicks, _opt.ElectionTimeoutMaxTicks + 1);
    }

    private void PersistHardState()
    {
        _store.SetHardState(Term, _votedFor);
        _dirty = true;
    }

    private void StartElection()
    {
        Term++;
        _votedFor = Id;
        Role = Role.Candidate;
        LeaderId = 0;
        PersistHardState();
        ResetElectionTimer();
        _votes.Clear();
        _votes.Add(Id);
        if (_votes.Count >= Majority)
        {
            BecomeLeader();
            return;
        }
        foreach (var p in _peers)
            Send(new RequestVote(Id, p.Id, Term, _log.LastIndex, _log.LastTerm));
    }

    private void HandleRequestVote(RequestVote m)
    {
        // Election restriction (5.4.1): only vote for a candidate whose log is at least as up-to-date.
        bool logOk = m.LastLogTerm > _log.LastTerm
                     || (m.LastLogTerm == _log.LastTerm && m.LastLogIndex >= _log.LastIndex);
        if (Has(InjectedBugs.VoteWithoutLogCheck)) logOk = true;

        bool granted = false;
        if (Role == Role.Follower && (_votedFor == 0 || _votedFor == m.From) && logOk)
        {
            _votedFor = m.From;
            PersistHardState();
            ResetElectionTimer();
            granted = true;
        }
        Send(new RequestVoteReply(Id, m.From, Term, granted));
    }

    private void HandleRequestVoteReply(RequestVoteReply m)
    {
        if (Role != Role.Candidate || !m.Granted) return;
        _votes.Add(m.From);
        if (_votes.Count >= Majority) BecomeLeader();
    }

    private void BecomeFollower(long term, int leader)
    {
        bool wasLeader = Role == Role.Leader;
        if (term > Term)
        {
            Term = term;
            _votedFor = 0;
            PersistHardState();
        }
        Role = Role.Follower;
        LeaderId = leader;
        ResetElectionTimer();
        if (wasLeader) FailPendingWork();
    }

    private void BecomeLeader()
    {
        Role = Role.Leader;
        LeaderId = Id;
        _heartbeatElapsed = 0;
        _electionElapsed = 0;
        _selfMatch = 0; // set again in Flush, after the log is on disk
        foreach (var p in _peers)
        {
            p.Match = 0;
            p.Next = _log.LastIndex + 1;
            p.State = PeerState.Probe;
            p.ProbeSent = false;
            p.AckSeq = 0;
            p.RecentActive = false;
        }
        _observer?.OnBecomeLeader(this);

        // A no-op in the new term lets the leader commit entries left by earlier terms (5.4.2) and
        // gives ReadIndex a point that is known to include every previously committed write.
        var noop = new LogEntry(Term, _log.LastIndex + 1, Array.Empty<byte>());
        AppendLocal(noop);
        _termStartIndex = noop.Index;
        _needReplicate = true;
    }

    /// <summary>Called when this node stops being leader: nobody waiting on it can be told anything certain.</summary>
    private void FailPendingWork()
    {
        var pending = _pending.Values.ToArray();
        _pending.Clear();
        foreach (var (_, cb) in pending) cb(new ProposeResult(ProposeStatus.Unknown, null, LeaderId));

        var reads = _reads.ToArray();
        _reads.Clear();
        foreach (var r in reads) r.Callback(ReadStatus.NotLeader, LeaderId);
        _readBroadcast = false;
        _needReplicate = false;
    }

    // ------------------------------------------------------------------ replication (follower side)

    private void HandleAppendEntries(AppendEntries m)
    {
        if (Role == Role.Leader) return; // impossible unless election safety is broken; the checker reports that
        if (Role == Role.Candidate) BecomeFollower(Term, m.From);
        LeaderId = m.From;
        _electionElapsed = 0;

        long prevIndex = m.PrevLogIndex;
        if (prevIndex > _log.LastIndex)
        {
            Send(new AppendEntriesReply(Id, m.From, Term, false, 0, _log.LastIndex + 1, m.Seq, prevIndex));
            return;
        }

        if (prevIndex >= _log.SnapshotIndex)
        {
            long ourTerm = _log.TermAt(prevIndex)!.Value;
            if (ourTerm != m.PrevLogTerm)
            {
                // Hint: back up to the first entry of the conflicting term (section 5.3 optimisation).
                long hint = prevIndex;
                while (hint - 1 > _log.SnapshotIndex && _log.TermAt(hint - 1) == ourTerm) hint--;
                Send(new AppendEntriesReply(Id, m.From, Term, false, 0, Math.Max(hint, CommitIndex + 1), m.Seq, prevIndex));
                return;
            }
        }
        // else: prevIndex is inside our snapshot. Those entries are committed, so they match the leader's.

        foreach (var e in m.Entries)
        {
            if (e.Index <= _log.SnapshotIndex) continue;
            if (e.Index <= _log.LastIndex)
            {
                if (_log.TermAt(e.Index) == e.Term) continue; // already have it (duplicate or reordered message)
                // Conflict: delete this entry and everything after it (Figure 2, receiver rule 3).
                _observer?.OnTruncate(Id, e.Index, CommitIndex);
                _log.TruncateFrom(e.Index);
                _store.TruncateFrom(e.Index);
                _dirty = true;
            }
            AppendLocal(e);
        }

        long lastNew = m.PrevLogIndex + m.Entries.Length;
        if (m.LeaderCommit > CommitIndex)
        {
            long newCommit = Math.Min(m.LeaderCommit, lastNew);
            if (newCommit > CommitIndex) AdvanceCommit(newCommit);
        }
        Send(new AppendEntriesReply(Id, m.From, Term, true, lastNew, 0, m.Seq));
    }

    private void HandleInstallSnapshot(InstallSnapshot m)
    {
        if (Role == Role.Leader) return;
        if (Role == Role.Candidate) BecomeFollower(Term, m.From);
        LeaderId = m.From;
        _electionElapsed = 0;

        if (m.LastIndex > CommitIndex)
        {
            var snapshot = new Snapshot(m.LastIndex, m.LastTerm, m.Data);
            _log.CompactTo(snapshot);
            _store.SaveSnapshot(snapshot);
            _dirty = true;
            _sm.Restore(m.Data);
            CommitIndex = LastApplied = m.LastIndex;
        }
        Send(new InstallSnapshotReply(Id, m.From, Term, m.LastIndex, m.Seq));
    }

    private void AppendLocal(LogEntry e)
    {
        long prevTerm = _log.LastTerm;
        _log.Append(e);
        _store.Append(e);
        _dirty = true;
        _observer?.OnAppend(Id, e, prevTerm);
    }

    // ------------------------------------------------------------------ replication (leader side)

    private Peer? FindPeer(int id)
    {
        foreach (var p in _peers) if (p.Id == id) return p;
        return null;
    }

    private void HandleAppendEntriesReply(AppendEntriesReply m)
    {
        if (Role != Role.Leader) return;
        var p = FindPeer(m.From);
        if (p is null) return;
        p.RecentActive = true;
        if (m.Seq > p.AckSeq) p.AckSeq = m.Seq;

        if (m.Success)
        {
            if (m.MatchIndex > p.Match) p.Match = m.MatchIndex;
            if (p.Next < p.Match + 1) p.Next = p.Match + 1;
            if (p.State == PeerState.Probe) p.State = PeerState.Replicate;
            p.ProbeSent = false;
            MaybeAdvanceCommit();
            SendTo(p, heartbeat: false);
        }
        else
        {
            // Only react to the rejection of the probe that is outstanding (or, while streaming, of an index
            // past what is known to match). Reacting to stale or duplicated rejections would let duplicates
            // multiply the probes in flight; a lost probe is retried by the heartbeat timer instead.
            if (p.State == PeerState.Snapshot) return;
            if (p.State == PeerState.Probe && m.RejectedPrevIndex != p.Next - 1) return;
            if (p.State == PeerState.Replicate && m.RejectedPrevIndex <= p.Match) return;
            long next = Math.Min(Math.Max(p.Match + 1, m.ConflictIndex), _log.LastIndex + 1);
            if (p.State == PeerState.Probe && p.ProbeSent && next == p.Next) return;
            p.Next = next;
            p.State = PeerState.Probe;
            p.ProbeSent = false;
            SendTo(p, heartbeat: false);
        }
    }

    private void HandleInstallSnapshotReply(InstallSnapshotReply m)
    {
        if (Role != Role.Leader) return;
        var p = FindPeer(m.From);
        if (p is null) return;
        p.RecentActive = true;
        if (m.Seq > p.AckSeq) p.AckSeq = m.Seq;
        if (m.LastIndex < 0) return; // answer to a stale-term snapshot from an earlier incarnation of this node
        if (p.State != PeerState.Snapshot && m.LastIndex <= p.Match) return; // duplicate or stale acknowledgement
        if (m.LastIndex > p.Match) p.Match = m.LastIndex;
        p.Next = p.Match + 1;
        p.State = PeerState.Probe;
        p.ProbeSent = false;
        MaybeAdvanceCommit();
        SendTo(p, heartbeat: false);
    }

    /// <summary>
    /// Sends what the peer needs next. Probe: one batch at a time until a reply shows where logs match.
    /// Replicate: pipeline batches up to the in-flight limit; heartbeats resend everything after Match,
    /// which also repairs lost batches. Snapshot: resend the snapshot on heartbeats until it is acknowledged.
    /// </summary>
    private void SendTo(Peer p, bool heartbeat)
    {
        switch (p.State)
        {
            case PeerState.Snapshot:
                // A snapshot can be large: resend it only if no answer came within an election timeout.
                if (heartbeat && _ticks - p.SnapshotSentAt >= _opt.ElectionTimeoutMinTicks) SendSnapshot(p);
                return;
            case PeerState.Probe:
                if (p.ProbeSent && !heartbeat) return;
                SendEntriesFrom(p, p.Next, advance: false);
                p.ProbeSent = true;
                return;
            case PeerState.Replicate:
                if (heartbeat)
                {
                    // Resending from Match+1 repairs lost batches. But right after a compaction Match+1 may be
                    // gone while the follower is only behind by what is in flight; sending it a whole snapshot
                    // for that would be waste. Resend from Next instead: if a batch really was lost, the
                    // follower rejects, the peer drops back to probing, and only then gets the snapshot.
                    long from = _log.TermAt(p.Match) is null ? p.Next : p.Match + 1;
                    SendEntriesFrom(p, from, advance: true);
                }
                else if (p.Next <= _log.LastIndex && p.Next - 1 - p.Match < _opt.MaxInflightEntries)
                    SendEntriesFrom(p, p.Next, advance: true);
                return;
        }
    }

    private void SendEntriesFrom(Peer p, long from, bool advance)
    {
        long prev = from - 1;
        long? prevTerm = _log.TermAt(prev);
        if (prevTerm is null)
        {
            // The entries this peer needs were compacted into the snapshot.
            SendSnapshot(p);
            return;
        }
        var entries = _log.Slice(from, _opt.MaxEntriesPerMessage);
        Send(new AppendEntries(Id, p.Id, Term, prev, prevTerm.Value, entries, CommitIndex, _heartbeatSeq));
        if (advance && entries.Length > 0) p.Next = Math.Max(p.Next, entries[^1].Index + 1);
    }

    private void SendSnapshot(Peer p)
    {
        p.State = PeerState.Snapshot;
        p.SnapshotSentAt = _ticks;
        var s = _log.Snapshot;
        Send(new InstallSnapshot(Id, p.Id, Term, s.LastIndex, s.LastTerm, s.Data, _heartbeatSeq));
    }

    /// <summary>
    /// Commit rule (5.3 and 5.4.2): the highest index stored on a majority becomes committed, but only if
    /// that entry is from the current term. Older entries then commit indirectly, by the Log Matching property.
    /// </summary>
    private void MaybeAdvanceCommit()
    {
        Span<long> matches = stackalloc long[_clusterSize];
        matches[0] = _selfMatch;
        for (int i = 0; i < _peers.Length; i++) matches[i + 1] = _peers[i].Match;
        matches.Sort();
        long candidate = matches[_clusterSize - Majority]; // the largest index that a majority has reached
        if (candidate <= CommitIndex) return;
        if (_log.TermAt(candidate) != Term && !Has(InjectedBugs.CommitPreviousTerm)) return;
        AdvanceCommit(candidate);
    }

    private void AdvanceCommit(long newCommit)
    {
        for (long i = CommitIndex + 1; i <= newCommit; i++) _observer?.OnCommit(Id, Term, _log.Get(i));
        CommitIndex = newCommit;
    }

    // ------------------------------------------------------------------ apply, reads, snapshots

    private void ApplyCommitted()
    {
        while (LastApplied < CommitIndex)
        {
            var e = _log.Get(LastApplied + 1);
            LastApplied = e.Index;
            object? result = e.IsNoOp ? null : _sm.Apply(e);
            _observer?.OnApply(Id, e);
            if (_pending.Remove(e.Index, out var p))
            {
                p.Callback(p.Term == e.Term
                    ? new ProposeResult(ProposeStatus.Applied, result, Id)
                    : new ProposeResult(ProposeStatus.Unknown, null, LeaderId));
            }
        }
        MaybeSnapshot();
    }

    private void MaybeSnapshot()
    {
        if (LastApplied - _log.SnapshotIndex < _opt.SnapshotThreshold) return;
        // Only applied (hence committed) entries are ever compacted.
        var snapshot = new Snapshot(LastApplied, _log.TermAt(LastApplied)!.Value, _sm.TakeSnapshot());
        _log.CompactTo(snapshot);
        _store.SaveSnapshot(snapshot);
        _dirty = true;
    }

    private void ConfirmReads()
    {
        foreach (var r in _reads)
        {
            if (r.Confirmed) continue;
            int acks = 1;
            foreach (var p in _peers) if (p.AckSeq >= r.Seq) acks++;
            if (acks >= Majority) r.Confirmed = true;
        }
    }

    private void CompleteReads()
    {
        if (_reads.Count == 0) return;
        List<PendingRead>? ready = null;
        for (int i = 0; i < _reads.Count; i++)
        {
            var r = _reads[i];
            if (r.Confirmed && r.ReadIndex <= LastApplied)
            {
                (ready ??= new()).Add(r);
                _reads.RemoveAt(i--);
            }
        }
        if (ready is null) return;
        foreach (var r in ready) r.Callback(ReadStatus.Ready, Id);
    }

    private void Send(RaftMessage m) => _outbox.Add(m);
}
