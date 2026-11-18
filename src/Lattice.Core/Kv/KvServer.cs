using Lattice.Raft;

namespace Lattice.Kv;

public enum ReadMode
{
    /// <summary>Reads use the ReadIndex barrier: no log write, one heartbeat round.</summary>
    ReadIndex,
    /// <summary>Reads are appended to the log like writes and answered when applied.</summary>
    Log,
}

public enum ClientStatus : byte
{
    Ok = 0,
    /// <summary>Not the leader. Nothing happened here; LeaderHint may say where to go.</summary>
    NotLeader = 1,
    /// <summary>The outcome is unknown (leadership changed). Retry with the same sequence number.</summary>
    Retry = 2,
}

public sealed record ClientRequest(long ClientId, long Seq, KvOp Op, string Key, string? Value, string? Expected);

public sealed record ClientResponse(long Seq, ClientStatus Status, bool Success, string? Value, int LeaderHint);

/// <summary>
/// Glue between client requests and Raft, shared by the simulator and the TCP server.
/// Writes become log entries; reads use ReadIndex or the log depending on <see cref="ReadMode"/>.
/// </summary>
public sealed class KvServer
{
    private readonly RaftNode _node;
    private readonly KvStateMachine _sm;
    private readonly ReadMode _readMode;

    public KvServer(RaftNode node, KvStateMachine sm, ReadMode readMode)
    {
        _node = node;
        _sm = sm;
        _readMode = readMode;
    }

    public RaftNode Node => _node;
    public KvStateMachine StateMachine => _sm;

    public void Handle(ClientRequest req, Action<ClientResponse> reply)
    {
        if (req.Op == KvOp.Get && _readMode == ReadMode.ReadIndex)
        {
            _node.ReadIndex((status, hint) =>
            {
                reply(status == ReadStatus.Ready
                    ? new ClientResponse(req.Seq, ClientStatus.Ok, true, _sm.Get(req.Key), _node.Id)
                    : new ClientResponse(req.Seq, ClientStatus.NotLeader, false, null, hint));
            });
            return;
        }

        var cmd = new KvCommand(req.Op, req.ClientId, req.Seq, req.Key, req.Value, req.Expected);
        _node.Propose(cmd.Encode(), r =>
        {
            switch (r.Status)
            {
                case ProposeStatus.Applied:
                    var result = (KvResult)r.Value!;
                    reply(new ClientResponse(req.Seq, ClientStatus.Ok, result.Success, result.Value, _node.Id));
                    break;
                case ProposeStatus.NotLeader:
                    reply(new ClientResponse(req.Seq, ClientStatus.NotLeader, false, null, r.LeaderHint));
                    break;
                default:
                    reply(new ClientResponse(req.Seq, ClientStatus.Retry, false, null, r.LeaderHint));
                    break;
            }
        });
    }
}
