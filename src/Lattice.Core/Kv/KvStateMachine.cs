using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Kv;

/// <summary>
/// The replicated key-value map plus the client session table that makes retries safe.
/// Each client sends one command at a time with increasing sequence numbers. If a command arrives
/// whose sequence number is not newer than the last one applied for that client, it is a retry (or a
/// duplicate delivered by the network) and the cached result is returned instead of applying it again.
/// The session table is part of the snapshot, so the guarantee survives compaction and restarts.
/// </summary>
public sealed class KvStateMachine : IStateMachine
{
    private readonly record struct Session(long Seq, KvResult Result);

    private readonly SortedDictionary<string, string> _data = new(StringComparer.Ordinal);
    private readonly SortedDictionary<long, Session> _sessions = new();
    private readonly bool _dedupe;

    public KvStateMachine(bool dedupe = true) => _dedupe = dedupe;

    public int Count => _data.Count;
    public int SessionCount => _sessions.Count;

    public string? Get(string key) => _data.TryGetValue(key, out var v) ? v : null;

    public object? Apply(LogEntry entry) => Apply(KvCommand.Decode(entry.Data));

    public KvResult Apply(KvCommand cmd)
    {
        if (_dedupe && _sessions.TryGetValue(cmd.ClientId, out var s) && cmd.Seq <= s.Seq)
            return s.Result;
        var result = Execute(cmd);
        _sessions[cmd.ClientId] = new Session(cmd.Seq, result);
        return result;
    }

    private KvResult Execute(KvCommand cmd)
    {
        switch (cmd.Op)
        {
            case KvOp.Get:
                return new KvResult(true, Get(cmd.Key));
            case KvOp.Put:
                _data[cmd.Key] = cmd.Value ?? throw new InvalidDataException("put without value");
                return new KvResult(true, null);
            case KvOp.Delete:
                return new KvResult(_data.Remove(cmd.Key), null);
            case KvOp.Cas:
                string? current = Get(cmd.Key);
                if (!string.Equals(current, cmd.Expected, StringComparison.Ordinal))
                    return new KvResult(false, current);
                if (cmd.Value is null) _data.Remove(cmd.Key);
                else _data[cmd.Key] = cmd.Value;
                return new KvResult(true, current);
            default:
                throw new InvalidDataException($"unknown op {cmd.Op}");
        }
    }

    public byte[] TakeSnapshot()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms))
        {
            w.Write(_data.Count);
            foreach (var (k, v) in _data)
            {
                w.Write(k);
                w.Write(v);
            }
            w.Write(_sessions.Count);
            foreach (var (client, s) in _sessions)
            {
                w.Write(client);
                w.Write(s.Seq);
                w.Write(s.Result.Success);
                w.WriteNullable(s.Result.Value);
            }
        }
        return ms.ToArray();
    }

    public void Restore(byte[] snapshot)
    {
        _data.Clear();
        _sessions.Clear();
        if (snapshot.Length == 0) return;
        using var r = new BinaryReader(new MemoryStream(snapshot));
        int n = r.ReadInt32();
        for (int i = 0; i < n; i++)
        {
            string k = r.ReadString();
            _data[k] = r.ReadString();
        }
        int sessions = r.ReadInt32();
        for (int i = 0; i < sessions; i++)
        {
            long client = r.ReadInt64();
            long seq = r.ReadInt64();
            bool ok = r.ReadBoolean();
            string? value = r.ReadNullableString();
            _sessions[client] = new Session(seq, new KvResult(ok, value));
        }
    }

    /// <summary>A fingerprint of the whole state (data and sessions), equal on replicas that applied the same prefix.</summary>
    public ulong Digest() => Hashing.Fnv64(TakeSnapshot());
}
