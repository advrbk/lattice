using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Lattice.Kv;

namespace Lattice.Net;

/// <summary>The outcome of one client call that got a definite answer.</summary>
public sealed record KvReply(bool Success, string? Value, int Leader);

/// <summary>Thrown when no definite answer arrived in time. The operation may or may not have happened.</summary>
public sealed class OutcomeUnknownException(string message) : Exception(message);

/// <summary>
/// A client session. It keeps one connection, follows leader hints, and retries a command with the
/// same (client id, sequence number) until it gets a definite answer, so the cluster applies it at most
/// once. Calls must not overlap: one outstanding command per session, as in section 6.3 of Ongaro's
/// dissertation. Use one client per concurrent caller.
/// </summary>
public sealed class LatticeClient : IAsyncDisposable
{
    private readonly IReadOnlyList<IPEndPoint> _nodes;
    private TcpClient? _socket;
    private NetworkStream? _stream;
    private int _connected = -1;
    private int _leader = -1;
    private int _rr;
    private long _seq;

    public LatticeClient(IReadOnlyList<IPEndPoint> nodes, long? clientId = null)
    {
        _nodes = nodes;
        ClientId = clientId ?? (RandomNumberGenerator.GetInt32(1, int.MaxValue) * 4_000_000_000L + RandomNumberGenerator.GetInt32(int.MaxValue));
        _rr = RandomNumberGenerator.GetInt32(nodes.Count);
    }

    public long ClientId { get; }
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(1);

    public Task<KvReply> PutAsync(string key, string value) => SendAsync(KvOp.Put, key, value, null);
    public Task<KvReply> GetAsync(string key) => SendAsync(KvOp.Get, key, null, null);
    public Task<KvReply> DeleteAsync(string key) => SendAsync(KvOp.Delete, key, null, null);
    public Task<KvReply> CasAsync(string key, string? expected, string? value) => SendAsync(KvOp.Cas, key, value, expected);

    public async Task<KvReply> SendAsync(KvOp op, string key, string? value, string? expected)
    {
        var req = new ClientRequest(ClientId, ++_seq, op, key, value, expected);
        var frame = Wire.Encode(req);
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            int target = _leader >= 0 ? _leader : _rr++ % _nodes.Count;
            try
            {
                var stream = await ConnectAsync(target);
                await stream.WriteAsync(frame);
                using var attempt = new CancellationTokenSource(AttemptTimeout);
                ClientResponse resp;
                do
                {
                    resp = await Wire.ReadAsync(stream, attempt.Token) as ClientResponse
                           ?? throw new IOException("connection closed");
                } while (resp.Seq != req.Seq);

                switch (resp.Status)
                {
                    case ClientStatus.Ok:
                        _leader = target;
                        return new KvReply(resp.Success, resp.Value, target + 1);
                    case ClientStatus.NotLeader:
                        _leader = resp.LeaderHint >= 1 && resp.LeaderHint <= _nodes.Count && resp.LeaderHint - 1 != target
                            ? resp.LeaderHint - 1 : -1;
                        if (_leader < 0) await Task.Delay(30);
                        break;
                    default:
                        _leader = -1;
                        await Task.Delay(30);
                        break;
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or InvalidDataException)
            {
                Disconnect();
                _leader = -1;
                await Task.Delay(30);
            }
        }
        throw new OutcomeUnknownException($"no answer for {op} {key} within {Timeout.TotalSeconds:0} s; it may or may not have been applied");
    }

    private async Task<NetworkStream> ConnectAsync(int index)
    {
        if (_connected == index && _stream is not null) return _stream;
        Disconnect();
        var socket = new TcpClient { NoDelay = true };
        using var cts = new CancellationTokenSource(AttemptTimeout);
        await socket.ConnectAsync(_nodes[index], cts.Token);
        _socket = socket;
        _stream = socket.GetStream();
        _connected = index;
        return _stream;
    }

    private void Disconnect()
    {
        _stream?.Dispose();
        _socket?.Dispose();
        _stream = null;
        _socket = null;
        _connected = -1;
    }

    /// <summary>Asks one node for its status; null if it does not answer.</summary>
    public static async Task<StatusResponse?> StatusAsync(IPEndPoint node, TimeSpan timeout)
    {
        try
        {
            using var cts = new CancellationTokenSource(timeout);
            using var socket = new TcpClient { NoDelay = true };
            await socket.ConnectAsync(node, cts.Token);
            var stream = socket.GetStream();
            await stream.WriteAsync(Wire.Encode(new StatusRequest()), cts.Token);
            return await Wire.ReadAsync(stream, cts.Token) as StatusResponse;
        }
        catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or InvalidDataException)
        {
            return null;
        }
    }

    public ValueTask DisposeAsync()
    {
        Disconnect();
        return ValueTask.CompletedTask;
    }

    public static List<IPEndPoint> ParseEndpoints(string list) =>
        list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(IPEndPoint.Parse).ToList();
}
