using Lattice.Util;

namespace Lattice.Kv;

public enum KvOp : byte { Get = 1, Put = 2, Delete = 3, Cas = 4 }

/// <summary>
/// A client command as stored in a log entry. (ClientId, Seq) identifies it for exactly-once
/// application: a retried command carries the same pair.
/// </summary>
public sealed record KvCommand(KvOp Op, long ClientId, long Seq, string Key, string? Value, string? Expected)
{
    public byte[] Encode()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms))
        {
            w.Write((byte)Op);
            w.Write(ClientId);
            w.Write(Seq);
            w.Write(Key);
            w.WriteNullable(Value);
            w.WriteNullable(Expected);
        }
        return ms.ToArray();
    }

    public static KvCommand Decode(byte[] data)
    {
        using var r = new BinaryReader(new MemoryStream(data));
        var op = (KvOp)r.ReadByte();
        long client = r.ReadInt64();
        long seq = r.ReadInt64();
        string key = r.ReadString();
        string? value = r.ReadNullableString();
        string? expected = r.ReadNullableString();
        return new KvCommand(op, client, seq, key, value, expected);
    }
}

/// <summary>
/// Result of applying a command. Get: Value is the value (null if absent). Put: Success is true.
/// Delete: Success says whether the key existed. Cas: Success says whether the swap happened and
/// Value is the value that was there before.
/// </summary>
public readonly record struct KvResult(bool Success, string? Value);
