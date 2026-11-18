using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Lattice.Kv;
using Lattice.Raft;
using Lattice.Storage;
using Lattice.Util;

namespace Lattice.Net;

public sealed class ServerConfig
{
    public required int Id { get; init; }
    /// <summary>Endpoints of every node, index 0 = node 1.</summary>
    public required IReadOnlyList<IPEndPoint> Cluster { get; init; }
    public required string DataDirectory { get; init; }
    public ReadMode ReadMode { get; init; } = ReadMode.ReadIndex;
    public int TickMilliseconds { get; init; } = 10;
    public RaftOptions Raft { get; init; } = new();
    public TextWriter? Log { get; init; }
}

/// <summary>
/// Runs one Raft node over TCP. All Raft and state machine work happens on a single event-loop
/// thread: network readers only decode frames and post them to the loop's queue, so the node itself
/// needs no locks (the same single-threaded model as in the simulator). After draining a batch of
/// events the loop calls Flush once, so one fsync covers every write in the batch (group commit).
/// </summary>
public sealed class ServerHost : IDisposable
{
    private readonly ServerConfig _config;
    private readonly BlockingCollection<Action> _inbox = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<int, PeerLink> _peers = new();
    private FileDurableStore? _store;
    private RaftNode? _node;
    private KvServer? _kv;
    private KvStateMachine? _sm;
    private TcpListener? _listener;
    private Thread? _loop;

    public ServerHost(ServerConfig config) => _config = config;

    public int Id => _config.Id;

    public void Start()
    {
        var ids = Enumerable.Range(1, _config.Cluster.Count).ToArray();
        _store = new FileDurableStore(_config.DataDirectory);
        _sm = new KvStateMachine();
        foreach (int id in ids)
            if (id != _config.Id)
                _peers[id] = new PeerLink(_config.Cluster[id - 1], _cts.Token);
        var transport = new TcpTransport(this);
        var seed = (ulong)Stopwatch.GetTimestamp() ^ ((ulong)_config.Id << 48) ^ (ulong)Environment.ProcessId;
        _node = new RaftNode(_config.Id, ids, _config.Raft, _store, _sm, transport, new DetRandom(seed));
        _kv = new KvServer(_node, _sm, _config.ReadMode);

        _listener = new TcpListener(_config.Cluster[_config.Id - 1]);
        _listener.Start(512);
        _ = AcceptLoop();
        _loop = new Thread(EventLoop) { IsBackground = true, Name = $"raft-{_config.Id}" };
        _loop.Start();
        _config.Log?.WriteLine($"node {_config.Id} listening on {_config.Cluster[_config.Id - 1]}, data in {_config.DataDirectory}, term {_node.Term}, log {_node.SnapshotIndex}..{_node.LastLogIndex}");
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener?.Stop();
        _inbox.CompleteAdding();
        _loop?.Join(2000);
        foreach (var p in _peers.Values) p.Dispose();
        _store?.Dispose();
    }

    private void Post(Action a)
    {
        try { _inbox.Add(a); }
        catch (InvalidOperationException) { /* shutting down */ }
    }

    // ------------------------------------------------------------------ event loop

    private void EventLoop()
    {
        var node = _node!;
        var clock = Stopwatch.StartNew();
        long tickMs = _config.TickMilliseconds;
        long nextTick = tickMs;
        Role lastRole = node.Role;
        long lastTerm = node.Term;
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                long wait = Math.Max(0, nextTick - clock.ElapsedMilliseconds);
                if (_inbox.TryTake(out var action, (int)Math.Min(wait, tickMs), _cts.Token))
                {
                    action();
                    for (int i = 0; i < 1024 && _inbox.TryTake(out action); i++) action();
                }
                long now = clock.ElapsedMilliseconds;
                if (now >= nextTick)
                {
                    long behind = (now - nextTick) / tickMs + 1;
                    for (long i = 0; i < Math.Min(behind, 5); i++) node.Tick();
                    nextTick += behind * tickMs;
                }
                node.Flush();
                if (node.Role != lastRole || node.Term != lastTerm)
                {
                    _config.Log?.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] node {node.Id}: {node.Role} in term {node.Term}");
                    lastRole = node.Role;
                    lastTerm = node.Term;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            // A failure inside Raft leaves the node in an unknown state; stop rather than limp on.
            _config.Log?.WriteLine($"node {_config.Id} event loop failed: {ex}");
            Environment.FailFast($"lattice node {_config.Id}: {ex.Message}", ex);
        }
    }

    private StatusResponse Status()
    {
        var n = _node!;
        return new StatusResponse(n.Id, n.Role, n.Term, n.LeaderId, n.CommitIndex, n.LastApplied, n.LastLogIndex, n.SnapshotIndex, _sm!.Count);
    }

    // ------------------------------------------------------------------ inbound connections

    private async Task AcceptLoop()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var socket = await _listener!.AcceptTcpClientAsync(_cts.Token);
                socket.NoDelay = true;
                _ = HandleConnection(socket);
            }
        }
        catch (Exception) when (_cts.IsCancellationRequested) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    private async Task HandleConnection(TcpClient socket)
    {
        using var _ = socket;
        var stream = socket.GetStream();
        var replies = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
        var writer = WriteReplies(stream, replies.Reader);
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var msg = await Wire.ReadAsync(stream, _cts.Token);
                if (msg is null) break;
                switch (msg)
                {
                    case RaftMessage m:
                        Post(() => _node!.Receive(m));
                        break;
                    case ClientRequest req:
                        Post(() => _kv!.Handle(req, resp => replies.Writer.TryWrite(Wire.Encode(resp))));
                        break;
                    case StatusRequest:
                        Post(() => replies.Writer.TryWrite(Wire.Encode(Status())));
                        break;
                }
            }
        }
        catch (Exception) { /* connection dropped or bad frame: close it */ }
        replies.Writer.TryComplete();
        try { await writer; } catch (Exception) { }
    }

    private static async Task WriteReplies(Stream stream, ChannelReader<byte[]> reader)
    {
        while (await reader.WaitToReadAsync())
        {
            while (reader.TryRead(out var frame)) await stream.WriteAsync(frame);
            await stream.FlushAsync();
        }
    }

    // ------------------------------------------------------------------ outbound (peer) connections

    private sealed class TcpTransport(ServerHost host) : IRaftTransport
    {
        public void Send(RaftMessage message)
        {
            if (host._peers.TryGetValue(message.To, out var link)) link.Enqueue(Wire.Encode(message));
        }
    }

    /// <summary>
    /// One outgoing connection to a peer, with its own queue and writer task. Messages are dropped if
    /// the queue overflows or the connection fails; Raft retries on its own, so the transport doesn't.
    /// </summary>
    private sealed class PeerLink : IDisposable
    {
        private readonly IPEndPoint _endpoint;
        private readonly CancellationToken _ct;
        private readonly Channel<byte[]> _queue = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(8192) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });

        public PeerLink(IPEndPoint endpoint, CancellationToken ct)
        {
            _endpoint = endpoint;
            _ct = ct;
            _ = Run();
        }

        public void Enqueue(byte[] frame) => _queue.Writer.TryWrite(frame);

        private async Task Run()
        {
            while (!_ct.IsCancellationRequested)
            {
                try
                {
                    using var client = new TcpClient { NoDelay = true };
                    using (var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(_ct))
                    {
                        connectTimeout.CancelAfter(500);
                        await client.ConnectAsync(_endpoint, connectTimeout.Token);
                    }
                    var stream = new BufferedStream(client.GetStream(), 1 << 16);
                    while (await _queue.Reader.WaitToReadAsync(_ct))
                    {
                        while (_queue.Reader.TryRead(out var frame)) await stream.WriteAsync(frame, _ct);
                        await stream.FlushAsync(_ct);
                    }
                }
                catch (Exception) when (!_ct.IsCancellationRequested)
                {
                    // Peer down or connection reset. Drop what is queued (it is stale by now) and retry.
                    while (_queue.Reader.TryRead(out _)) { }
                    try { await Task.Delay(100, _ct); } catch (OperationCanceledException) { }
                }
                catch (Exception) { }
            }
        }

        public void Dispose() => _queue.Writer.TryComplete();
    }
}
