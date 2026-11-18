using Lattice.Check;
using Lattice.Kv;
using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Sim;

public sealed class SimResult
{
    public ulong Seed { get; init; }
    public bool Passed => Failure is null;
    /// <summary>Invariant name, "Linearizability", "Convergence", "CheckerGaveUp" or "Exception".</summary>
    public string? FailureKind { get; set; }
    public string? Failure { get; set; }
    public long Events { get; set; }
    public long SimMicros { get; set; }
    public int OpsCompleted { get; set; }
    public int OpsUnknown { get; set; }
    /// <summary>Faults in the schedule.</summary>
    public int Faults { get; set; }
    /// <summary>
    /// Faults that actually took effect. Some scheduled faults change nothing: crashing or pausing a node
    /// that is already down, isolating, killing or pausing "the leader" when there is none, or a
    /// new-leader kill that never sees a new leader before the workload ends.
    /// </summary>
    public int FaultsFired { get; set; }
    public int Elections { get; set; }
    public long Commits { get; set; }
    public long MessagesSent { get; set; }
    public long MessagesDropped { get; set; }
    public long SnapshotsSent { get; set; }
    public long Crashes { get; set; }
    public long CheckerSteps { get; set; }
    public ulong TraceHash { get; set; }
    public List<Operation> History { get; set; } = new();
}

/// <summary>
/// The deterministic simulator. Everything (node clocks, the network, disks, clients and the fault
/// schedule) is driven by one priority queue of timed events on a single thread, and every random
/// choice comes from generators seeded by the run's seed. Two runs with the same seed and options
/// therefore process exactly the same events in the same order; <see cref="SimResult.TraceHash"/>
/// fingerprints that order so tests can prove it.
/// </summary>
public sealed class Simulation
{
    private enum EvKind { Deliver, Tick, Writeback, ClientStart, ClientTimeout, ClientRetry, FaultStart, FaultEnd, KillArmed, WorkloadEnd }

    private readonly record struct Ev(EvKind Kind, int A, int B, object? Payload);

    private sealed record Packet(int From, int To, object Payload);

    private sealed class Server
    {
        public required int Id;
        public SimDisk Disk = new();
        public RaftNode? Node;
        public KvStateMachine? Sm;
        public KvServer? Kv;
        public bool Up;
        public int Incarnation;
        public long PausedUntil;
        public double Drift;
    }

    private sealed class Client
    {
        public required int Index;
        public required int Endpoint;
        public required long ClientId;
        public long Seq;
        public Operation? Current;
        public ClientRequest? Request;
        public long Deadline;
        public int Target;
        public int LeaderHint;
        public int Token;
        public readonly Dictionary<string, string?> LastSeen = new();
    }

    private const int ClientEndpointBase = 1000;

    private readonly ulong _seed;
    private readonly SimOptions _o;
    private readonly IReadOnlyList<Fault> _faults;
    private readonly TextWriter? _trace;
    private readonly DetRandom _rng;
    private readonly PriorityQueue<Ev, (long, long)> _queue = new();
    private readonly Server[] _servers;
    private readonly Client[] _clients;
    private readonly int[] _ids;
    private readonly int[,] _blocked;
    private readonly RaftInvariants _invariants;
    private readonly List<Operation> _history = new();
    private readonly bool[] _faultActive;
    private readonly List<(int, int)>?[] _faultLinks;
    private readonly int[] _faultTarget;
    private readonly Queue<int> _armedLeaderKills = new();

    private long _now;
    private long _eventSeq;
    private long _stamp;
    private bool _workloadOver;
    private double _extraDrop;
    private double _extraDup;
    private long _extraDelay;
    private ulong _traceHash = 14695981039346656037UL;
    private long _sent;
    private long _dropped;
    private long _snapshotsSent;
    private long _crashes;
    private int _faultsFired;

    public Simulation(ulong seed, SimOptions options, IReadOnlyList<Fault>? faults = null, TextWriter? trace = null)
    {
        _seed = seed;
        _o = options;
        _faults = faults ?? FaultPlan.Generate(seed, options);
        _trace = trace;
        _rng = new DetRandom(seed);
        _ids = Enumerable.Range(1, options.Nodes).ToArray();
        _servers = new Server[options.Nodes + 1];
        for (int i = 1; i <= options.Nodes; i++) _servers[i] = new Server { Id = i };
        _blocked = new int[options.Nodes + 1, options.Nodes + 1];
        _clients = new Client[options.Clients];
        for (int i = 0; i < options.Clients; i++)
            _clients[i] = new Client { Index = i, Endpoint = ClientEndpointBase + i, ClientId = (long)(seed % 100_000) * 100 + i + 1 };
        _faultActive = new bool[_faults.Count];
        _faultLinks = new List<(int, int)>?[_faults.Count];
        _faultTarget = new int[_faults.Count];
        _invariants = new RaftInvariants(options.CheckInvariants);
    }

    /// <summary>A run that needs more events than this is reported as a failure instead of running forever.</summary>
    public const long MaxEvents = 5_000_000;

    public IReadOnlyList<Fault> Faults => _faults;

    public SimResult Run()
    {
        var result = new SimResult { Seed = _seed, Faults = _faults.Count };
        long end = _o.WorkloadMicros + _o.QuietMicros;
        try
        {
            foreach (var s in _servers.Skip(1)) StartServer(s!);
            foreach (var c in _clients) Schedule(_rng.NextLong(0, 50_000), new Ev(EvKind.ClientStart, c.Index, 0, null));
            for (int i = 0; i < _faults.Count; i++) Schedule(_faults[i].AtMicros, new Ev(EvKind.FaultStart, i, 0, null));
            Schedule(_o.WorkloadMicros, new Ev(EvKind.WorkloadEnd, 0, 0, null));

            while (_queue.TryDequeue(out var ev, out var pri))
            {
                if (pri.Item1 > end) break;
                _now = pri.Item1;
                if (++result.Events > MaxEvents)
                    throw new InvariantViolation("EventLimit", $"more than {MaxEvents:N0} events: a message storm or livelock");
                _traceHash = Hashing.Mix(Hashing.Mix(Hashing.Mix(_traceHash, (ulong)_now), (ulong)ev.Kind), ((ulong)(uint)ev.A << 32) | (uint)ev.B);
                Dispatch(ev);
            }
            _now = end;

            // Anything still outstanding never got an answer: its outcome is unknown.
            foreach (var c in _clients)
                if (c.Current is not null) Finish(c, null);

            CheckConvergence();
        }
        catch (InvariantViolation v)
        {
            result.FailureKind = v.Invariant;
            result.Failure = $"{v.Message} (at {_now / 1000.0:0.000} ms simulated)";
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            result.FailureKind = ex is ConvergenceException ? "Convergence" : "Exception";
            result.Failure = ex is ConvergenceException ? ex.Message : $"{ex.GetType().Name}: {ex.Message} (at {_now / 1000.0:0.000} ms)\n{ex.StackTrace}";
        }

        result.SimMicros = _now;
        result.Elections = _invariants.Elections;
        result.Commits = _invariants.MaxCommitted;
        result.MessagesSent = _sent;
        result.MessagesDropped = _dropped;
        result.SnapshotsSent = _snapshotsSent;
        result.Crashes = _crashes;
        result.FaultsFired = _faultsFired;
        result.History = _history;
        result.OpsCompleted = _history.Count(o => o.Completed);
        result.OpsUnknown = _history.Count(o => !o.Completed);
        foreach (var s in _servers.Skip(1))
            if (s!.Up) _traceHash = Hashing.Mix(_traceHash, s.Sm!.Digest());
        result.TraceHash = _traceHash;

        if (result.Passed)
        {
            var check = LinearizabilityChecker.Check(_history);
            result.CheckerSteps = check.StatesExplored;
            if (check.Outcome == CheckOutcome.NotLinearizable)
            {
                result.FailureKind = "Linearizability";
                result.Failure = check.Explanation;
            }
            else if (check.Outcome == CheckOutcome.GaveUp)
            {
                result.FailureKind = "CheckerGaveUp";
                result.Failure = check.Explanation;
            }
        }
        return result;
    }

    // ------------------------------------------------------------------ event loop

    private void Schedule(long at, Ev ev) => _queue.Enqueue(ev, (at, _eventSeq++));

    private void Trace(string message)
    {
        _trace?.WriteLine($"{_now / 1000.0,10:0.000} ms  {message}");
    }

    private void Dispatch(Ev ev)
    {
        switch (ev.Kind)
        {
            case EvKind.Deliver: Deliver((Packet)ev.Payload!, ev); break;
            case EvKind.Tick: OnTick(ev); break;
            case EvKind.Writeback: OnWriteback(ev); break;
            case EvKind.ClientStart: StartOperation(_clients[ev.A]); break;
            case EvKind.ClientTimeout: OnClientTimer(_clients[ev.A], ev.B, timedOut: true); break;
            case EvKind.ClientRetry: OnClientTimer(_clients[ev.A], ev.B, timedOut: false); break;
            case EvKind.FaultStart: StartFault(ev.A); break;
            case EvKind.FaultEnd: EndFault(ev.A); break;
            case EvKind.KillArmed: KillArmed(ev.A, ev.B); break;
            case EvKind.WorkloadEnd: EndWorkload(); break;
        }
    }

    // ------------------------------------------------------------------ servers

    private void StartServer(Server s)
    {
        s.Incarnation++;
        s.Up = true;
        s.PausedUntil = 0;
        s.Drift = (_rng.NextDouble() * 2 - 1) * _o.MaxClockDrift;
        s.Sm = new KvStateMachine(dedupe: !_o.Raft.Has(InjectedBugs.NoDedupe));
        var transport = new SimTransport(this, s.Id);
        s.Node = new RaftNode(s.Id, _ids, _o.Raft, s.Disk, s.Sm, transport, _rng.Fork(),
            _invariants);
        s.Kv = new KvServer(s.Node, s.Sm, _o.ReadMode);
        Trace($"node {s.Id} starts (term {s.Node.Term}, log {s.Node.SnapshotIndex}..{s.Node.LastLogIndex})");
        Schedule(_now + _rng.NextLong(0, _o.TickMicros), new Ev(EvKind.Tick, s.Id, s.Incarnation, null));
        Schedule(_now + _rng.NextLong(_o.WritebackMinMicros, _o.WritebackMaxMicros), new Ev(EvKind.Writeback, s.Id, s.Incarnation, null));
    }

    private void CrashServer(Server s)
    {
        if (!s.Up) return;
        int lost = s.Disk.UnsyncedWrites;
        _crashes++;
        s.Up = false;
        s.Node = null;
        s.Kv = null;
        s.Sm = null;
        s.Incarnation++;
        s.Disk.Crash(_rng);
        Trace($"node {s.Id} crashes ({lost} unsynced disk writes at risk)");
    }

    private void OnTick(Ev ev)
    {
        var s = _servers[ev.A];
        if (!s.Up || s.Incarnation != ev.B) return;
        if (_now < s.PausedUntil)
        {
            Schedule(s.PausedUntil, ev);
            return;
        }
        var node = s.Node!;
        var roleBefore = node.Role;
        long termBefore = node.Term;
        node.Tick();
        node.Flush();
        AfterStep(s, roleBefore, termBefore);
        long interval = (long)(_o.TickMicros * (1 + s.Drift)) + _rng.NextLong(-_o.TickJitterMicros, _o.TickJitterMicros + 1);
        Schedule(_now + Math.Max(1, interval), ev);
    }

    private void OnWriteback(Ev ev)
    {
        var s = _servers[ev.A];
        if (!s.Up || s.Incarnation != ev.B) return;
        s.Disk.Sync();
        Schedule(_now + _rng.NextLong(_o.WritebackMinMicros, _o.WritebackMaxMicros), ev);
    }

    private int CurrentLeader()
    {
        // The up, unpaused leader with the highest term (there can be stale leaders in old terms).
        int best = 0;
        long bestTerm = -1;
        foreach (var s in _servers.Skip(1))
            if (s!.Up && s.Node!.Role == Role.Leader && s.Node.Term > bestTerm)
            {
                best = s.Id;
                bestTerm = s.Node.Term;
            }
        return best;
    }

    // ------------------------------------------------------------------ network

    private sealed class SimTransport : IRaftTransport
    {
        private readonly Simulation _sim;
        private readonly int _from;

        public SimTransport(Simulation sim, int from)
        {
            _sim = sim;
            _from = from;
        }

        public void Send(RaftMessage message) => _sim.Send(_from, message.To, message);
    }

    private bool IsServer(int endpoint) => endpoint >= 1 && endpoint <= _o.Nodes;

    private bool Blocked(int from, int to) => IsServer(from) && IsServer(to) && _blocked[from, to] > 0;

    private void Send(int from, int to, object payload)
    {
        _sent++;
        if (payload is InstallSnapshot snap)
        {
            _snapshotsSent++;
            if (_trace is not null) Trace($"node {from} sends a snapshot up to {snap.LastIndex} to node {to}");
        }
        if (Blocked(from, to) || _rng.Chance(_o.DropRate + _extraDrop))
        {
            _dropped++;
            return;
        }
        var packet = new Packet(from, to, payload);
        Schedule(_now + Delay(), new Ev(EvKind.Deliver, to, 0, packet));
        if (_rng.Chance(_o.DuplicateRate + _extraDup))
            Schedule(_now + Delay(), new Ev(EvKind.Deliver, to, 0, packet));
    }

    private long Delay()
    {
        long d = _rng.NextLong(_o.MinDelayMicros, _o.MaxDelayMicros + 1);
        if (_extraDelay > 0) d += _rng.NextLong(0, _extraDelay);
        if (_rng.Chance(_o.ReorderRate)) d += _rng.NextLong(0, _o.ReorderMaxMicros);
        return d;
    }

    private void Deliver(Packet p, Ev ev)
    {
        if (Blocked(p.From, p.To))
        {
            _dropped++;
            return; // a partition also cuts messages already in flight
        }
        if (!IsServer(p.To))
        {
            OnClientResponse(_clients[p.To - ClientEndpointBase], (ClientResponse)p.Payload);
            return;
        }
        var s = _servers[p.To];
        if (!s.Up)
        {
            _dropped++;
            return;
        }
        if (_now < s.PausedUntil)
        {
            Schedule(s.PausedUntil + _rng.NextLong(0, 1_000), ev);
            return;
        }
        var node = s.Node!;
        var roleBefore = node.Role;
        long termBefore = node.Term;
        if (p.Payload is RaftMessage m)
        {
            node.Receive(m);
        }
        else
        {
            var req = (ClientRequest)p.Payload;
            int replyTo = p.From;
            int serverId = s.Id;
            s.Kv!.Handle(req, resp => Send(serverId, replyTo, resp));
        }
        node.Flush();
        AfterStep(s, roleBefore, termBefore);
    }

    private void AfterStep(Server s, Role roleBefore, long termBefore)
    {
        var node = s.Node!;
        if (node.Role == roleBefore && node.Term == termBefore) return;
        if (_trace is not null) Trace($"node {s.Id} is {node.Role} in term {node.Term}");
        if (node.Role == Role.Leader && _armedLeaderKills.Count > 0 && !_workloadOver)
        {
            int i = _armedLeaderKills.Dequeue();
            _faultTarget[i] = s.Id;
            Schedule(_now + (long)_faults[i].Amount, new Ev(EvKind.KillArmed, i, s.Incarnation, null));
        }
    }

    private void KillArmed(int i, int incarnation)
    {
        var s = _servers[_faultTarget[i]];
        if (_workloadOver || !s.Up || s.Incarnation != incarnation) return;
        CrashServer(s);
        _faultActive[i] = true;
        _faultsFired++;
        Trace($"fault fires: {_faults[i]} (node {s.Id})");
        Schedule(_now + _faults[i].DurationMicros, new Ev(EvKind.FaultEnd, i, 0, null));
    }

    // ------------------------------------------------------------------ faults

    private void StartFault(int i)
    {
        if (_workloadOver) return;
        var f = _faults[i];
        switch (f.Kind)
        {
            case FaultKind.Partition:
                Partition(i, f.Group);
                break;
            case FaultKind.IsolateLeader:
                int leader = CurrentLeader();
                if (leader == 0) return;
                Partition(i, new[] { leader });
                break;
            case FaultKind.Crash:
            case FaultKind.KillLeader:
                int victim = f.Kind == FaultKind.Crash ? f.Target : CurrentLeader();
                if (victim == 0 || !_servers[victim].Up) return;
                CrashServer(_servers[victim]);
                _faultTarget[i] = victim;
                break;
            case FaultKind.KillNewLeader:
                _armedLeaderKills.Enqueue(i);
                Trace($"fault armed: {f}");
                return; // fires from AfterStep when the next leader is elected
            case FaultKind.Pause:
            case FaultKind.PauseLeader:
                int paused = f.Kind == FaultKind.Pause ? f.Target : CurrentLeader();
                if (paused == 0 || !_servers[paused].Up) return;
                _servers[paused].PausedUntil = Math.Max(_servers[paused].PausedUntil, _now + f.DurationMicros);
                _faultsFired++;
                Trace($"node {paused} pauses for {f.DurationMicros / 1000.0:0.0} ms");
                return; // a pause ends by itself
            case FaultKind.LossBurst:
                _extraDrop += f.Amount;
                break;
            case FaultKind.DuplicateBurst:
                _extraDup += f.Amount;
                break;
            case FaultKind.SlowNetwork:
                _extraDelay += (long)f.Amount;
                break;
        }
        _faultActive[i] = true;
        _faultsFired++;
        Trace($"fault starts: {f}");
        Schedule(_now + f.DurationMicros, new Ev(EvKind.FaultEnd, i, 0, null));
    }

    private void Partition(int faultIndex, int[] group)
    {
        var links = new List<(int, int)>();
        foreach (int a in group)
            foreach (int b in _ids)
                if (!group.Contains(b))
                {
                    links.Add((a, b));
                    links.Add((b, a));
                }
        foreach (var (a, b) in links) _blocked[a, b]++;
        _faultLinks[faultIndex] = links;
        Trace($"partition: {{{string.Join(",", group)}}} cut off");
    }

    private void EndFault(int i)
    {
        if (!_faultActive[i]) return;
        _faultActive[i] = false;
        var f = _faults[i];
        switch (f.Kind)
        {
            case FaultKind.Partition:
            case FaultKind.IsolateLeader:
                foreach (var (a, b) in _faultLinks[i]!) _blocked[a, b]--;
                break;
            case FaultKind.Crash:
            case FaultKind.KillLeader:
            case FaultKind.KillNewLeader:
                var s = _servers[_faultTarget[i]];
                if (!s.Up) StartServer(s);
                break;
            case FaultKind.LossBurst:
                _extraDrop -= f.Amount;
                break;
            case FaultKind.DuplicateBurst:
                _extraDup -= f.Amount;
                break;
            case FaultKind.SlowNetwork:
                _extraDelay -= (long)f.Amount;
                break;
        }
        Trace($"fault ends: [{i}] {f.Kind}");
    }

    private void EndWorkload()
    {
        _workloadOver = true;
        for (int i = 0; i < _faults.Count; i++) EndFault(i);
        foreach (var s in _servers.Skip(1))
        {
            if (!s!.Up) StartServer(s);
            s.PausedUntil = Math.Min(s.PausedUntil, _now);
        }
        _extraDrop = _extraDup = 0;
        _extraDelay = 0;
        Trace("workload over: all faults healed, every node up");
    }

    // ------------------------------------------------------------------ clients

    private long NextStamp() => ++_stamp;

    private void StartOperation(Client c)
    {
        if (_workloadOver) return;
        c.Seq++;
        string key = "k" + _rng.Next(_o.Keys);
        string unique = $"{c.Index}.{c.Seq}";
        int roll = _rng.Next(100);
        Operation op;
        ClientRequest req;
        if (roll < 40)
        {
            op = new Operation { Client = c.Index, Key = key, Kind = OpKind.Get };
            req = new ClientRequest(c.ClientId, c.Seq, KvOp.Get, key, null, null);
        }
        else if (roll < 70)
        {
            op = new Operation { Client = c.Index, Key = key, Kind = OpKind.Put, Value = unique };
            req = new ClientRequest(c.ClientId, c.Seq, KvOp.Put, key, unique, null);
        }
        else if (roll < 80)
        {
            op = new Operation { Client = c.Index, Key = key, Kind = OpKind.Delete };
            req = new ClientRequest(c.ClientId, c.Seq, KvOp.Delete, key, null, null);
        }
        else
        {
            // Compare-and-swap from the value this client last saw, so a fair share of them succeed.
            c.LastSeen.TryGetValue(key, out var expected);
            op = new Operation { Client = c.Index, Key = key, Kind = OpKind.Cas, Expected = expected, Value = unique };
            req = new ClientRequest(c.ClientId, c.Seq, KvOp.Cas, key, unique, expected);
        }
        op.Invoke = NextStamp();
        _history.Add(op);
        c.Current = op;
        c.Request = req;
        c.Deadline = _now + _o.OperationTimeoutMicros;
        SendAttempt(c);
    }

    private void SendAttempt(Client c)
    {
        c.Target = c.LeaderHint > 0 ? c.LeaderHint : _rng.Next(1, _o.Nodes + 1);
        c.Token++;
        Send(c.Endpoint, c.Target, c.Request!);
        Schedule(_now + _rng.NextLong(80_000, 200_000), new Ev(EvKind.ClientTimeout, c.Index, c.Token, null));
    }

    private void OnClientResponse(Client c, ClientResponse resp)
    {
        if (c.Current is null || resp.Seq != c.Seq) return; // a late answer to an earlier operation
        switch (resp.Status)
        {
            case ClientStatus.Ok:
                Finish(c, resp);
                break;
            case ClientStatus.NotLeader:
            case ClientStatus.Retry:
                c.LeaderHint = resp.LeaderHint != c.Target ? resp.LeaderHint : 0;
                c.Token++;
                long backoff = resp.LeaderHint > 0 && resp.LeaderHint != c.Target ? _rng.NextLong(500, 3_000) : _rng.NextLong(10_000, 30_000);
                Schedule(_now + backoff, new Ev(EvKind.ClientRetry, c.Index, c.Token, null));
                break;
        }
    }

    private void OnClientTimer(Client c, int token, bool timedOut)
    {
        if (c.Current is null || token != c.Token) return;
        if (_now >= c.Deadline)
        {
            Finish(c, null);
            return;
        }
        if (timedOut) c.LeaderHint = 0;
        SendAttempt(c);
    }

    /// <summary>Records the outcome (null = unknown) and schedules the client's next operation.</summary>
    private void Finish(Client c, ClientResponse? resp)
    {
        var op = c.Current!;
        if (resp is not null)
        {
            op.Return = NextStamp();
            if (_trace is not null) Trace($"client {c.Index} answered by node {resp.LeaderHint}: {op.Kind} seq {c.Seq} value {resp.Value ?? "nil"} ok {resp.Success} (op #{_history.IndexOf(op)})");
            switch (op.Kind)
            {
                case OpKind.Get:
                    op.Output = resp.Value;
                    c.LastSeen[op.Key] = resp.Value;
                    break;
                case OpKind.Put:
                    c.LastSeen[op.Key] = op.Value;
                    break;
                case OpKind.Delete:
                    c.LastSeen[op.Key] = null;
                    break;
                case OpKind.Cas:
                    op.CasSucceeded = resp.Success;
                    if (!resp.Success) op.Output = resp.Value;
                    c.LastSeen[op.Key] = resp.Success ? op.Value : resp.Value;
                    break;
            }
        }
        c.Current = null;
        c.Request = null;
        c.Token++;
        if (resp is null) c.LeaderHint = 0;
        Schedule(_now + _rng.NextLong(0, 10_000), new Ev(EvKind.ClientStart, c.Index, 0, null));
    }

    // ------------------------------------------------------------------ end-of-run checks

    private sealed class ConvergenceException(string message) : Exception(message);

    /// <summary>Replicas that applied the same number of entries must hold identical state.</summary>
    private void CheckConvergence()
    {
        var byIndex = new Dictionary<long, (int Node, ulong Digest)>();
        foreach (var s in _servers.Skip(1))
        {
            if (!s!.Up) continue;
            ulong digest = s.Sm!.Digest();
            long applied = s.Node!.LastApplied;
            if (byIndex.TryGetValue(applied, out var other) && other.Digest != digest)
                throw new ConvergenceException($"nodes {other.Node} and {s.Id} both applied {applied} entries but hold different state");
            byIndex[applied] = (s.Id, digest);
        }
    }
}
