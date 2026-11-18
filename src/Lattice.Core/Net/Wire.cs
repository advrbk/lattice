using System.Buffers.Binary;
using Lattice.Kv;
using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Net;

/// <summary>Asks a node for its view of the cluster.</summary>
public sealed record StatusRequest;

public sealed record StatusResponse(int Id, Role Role, long Term, int LeaderId, long CommitIndex, long LastApplied,
    long LastLogIndex, long SnapshotIndex, int Keys);

/// <summary>
/// The TCP wire format. Every frame is <c>[int32 length][byte type][payload]</c>, little-endian, with
/// payloads written by BinaryWriter. Node-to-node and client traffic share the format.
/// </summary>
public static class Wire
{
    public const int MaxFrame = 64 << 20;

    private const byte TRequestVote = 1, TRequestVoteReply = 2, TAppendEntries = 3, TAppendEntriesReply = 4,
        TInstallSnapshot = 5, TInstallSnapshotReply = 6, TClientRequest = 20, TClientResponse = 21,
        TStatusRequest = 22, TStatusResponse = 23;

    public static byte[] Encode(object message)
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms))
        {
            w.Write(0); // length placeholder
            switch (message)
            {
                case RequestVote m:
                    w.Write(TRequestVote); Header(w, m); w.Write(m.LastLogIndex); w.Write(m.LastLogTerm);
                    break;
                case RequestVoteReply m:
                    w.Write(TRequestVoteReply); Header(w, m); w.Write(m.Granted);
                    break;
                case AppendEntries m:
                    w.Write(TAppendEntries); Header(w, m);
                    w.Write(m.PrevLogIndex); w.Write(m.PrevLogTerm); w.Write(m.LeaderCommit); w.Write(m.Seq);
                    w.Write(m.Entries.Length);
                    foreach (var e in m.Entries)
                    {
                        w.Write(e.Term); w.Write(e.Index); w.WriteBytes(e.Data);
                    }
                    break;
                case AppendEntriesReply m:
                    w.Write(TAppendEntriesReply); Header(w, m);
                    w.Write(m.Success); w.Write(m.MatchIndex); w.Write(m.ConflictIndex); w.Write(m.Seq); w.Write(m.RejectedPrevIndex);
                    break;
                case InstallSnapshot m:
                    w.Write(TInstallSnapshot); Header(w, m);
                    w.Write(m.LastIndex); w.Write(m.LastTerm); w.Write(m.Seq); w.WriteBytes(m.Data);
                    break;
                case InstallSnapshotReply m:
                    w.Write(TInstallSnapshotReply); Header(w, m); w.Write(m.LastIndex); w.Write(m.Seq);
                    break;
                case ClientRequest m:
                    w.Write(TClientRequest);
                    w.Write(m.ClientId); w.Write(m.Seq); w.Write((byte)m.Op); w.Write(m.Key);
                    w.WriteNullable(m.Value); w.WriteNullable(m.Expected);
                    break;
                case ClientResponse m:
                    w.Write(TClientResponse);
                    w.Write(m.Seq); w.Write((byte)m.Status); w.Write(m.Success); w.WriteNullable(m.Value); w.Write(m.LeaderHint);
                    break;
                case StatusRequest:
                    w.Write(TStatusRequest);
                    break;
                case StatusResponse m:
                    w.Write(TStatusResponse);
                    w.Write(m.Id); w.Write((byte)m.Role); w.Write(m.Term); w.Write(m.LeaderId); w.Write(m.CommitIndex);
                    w.Write(m.LastApplied); w.Write(m.LastLogIndex); w.Write(m.SnapshotIndex); w.Write(m.Keys);
                    break;
                default:
                    throw new ArgumentException($"cannot encode {message.GetType().Name}");
            }
        }
        var frame = ms.ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(frame, frame.Length - 4);
        return frame;
    }

    private static void Header(BinaryWriter w, RaftMessage m)
    {
        w.Write(m.From); w.Write(m.To); w.Write(m.Term);
    }

    /// <summary>Decodes a frame body (everything after the length prefix).</summary>
    public static object Decode(byte[] body)
    {
        using var r = new BinaryReader(new MemoryStream(body));
        byte type = r.ReadByte();
        switch (type)
        {
            case TRequestVote:
            {
                var (f, t, term) = ReadHeader(r);
                return new RequestVote(f, t, term, r.ReadInt64(), r.ReadInt64());
            }
            case TRequestVoteReply:
            {
                var (f, t, term) = ReadHeader(r);
                return new RequestVoteReply(f, t, term, r.ReadBoolean());
            }
            case TAppendEntries:
            {
                var (f, t, term) = ReadHeader(r);
                long prevIndex = r.ReadInt64(), prevTerm = r.ReadInt64(), commit = r.ReadInt64(), seq = r.ReadInt64();
                int n = r.ReadInt32();
                if (n < 0 || n > 1_000_000) throw new InvalidDataException("bad entry count");
                var entries = new LogEntry[n];
                for (int i = 0; i < n; i++)
                {
                    long et = r.ReadInt64(), ei = r.ReadInt64();
                    entries[i] = new LogEntry(et, ei, r.ReadBytes32());
                }
                return new AppendEntries(f, t, term, prevIndex, prevTerm, entries, commit, seq);
            }
            case TAppendEntriesReply:
            {
                var (f, t, term) = ReadHeader(r);
                return new AppendEntriesReply(f, t, term, r.ReadBoolean(), r.ReadInt64(), r.ReadInt64(), r.ReadInt64(), r.ReadInt64());
            }
            case TInstallSnapshot:
            {
                var (f, t, term) = ReadHeader(r);
                long last = r.ReadInt64(), lastTerm = r.ReadInt64(), seq = r.ReadInt64();
                return new InstallSnapshot(f, t, term, last, lastTerm, r.ReadBytes32(), seq);
            }
            case TInstallSnapshotReply:
            {
                var (f, t, term) = ReadHeader(r);
                return new InstallSnapshotReply(f, t, term, r.ReadInt64(), r.ReadInt64());
            }
            case TClientRequest:
                return new ClientRequest(r.ReadInt64(), r.ReadInt64(), (KvOp)r.ReadByte(), r.ReadString(),
                    r.ReadNullableString(), r.ReadNullableString());
            case TClientResponse:
                return new ClientResponse(r.ReadInt64(), (ClientStatus)r.ReadByte(), r.ReadBoolean(), r.ReadNullableString(), r.ReadInt32());
            case TStatusRequest:
                return new StatusRequest();
            case TStatusResponse:
                return new StatusResponse(r.ReadInt32(), (Role)r.ReadByte(), r.ReadInt64(), r.ReadInt32(), r.ReadInt64(),
                    r.ReadInt64(), r.ReadInt64(), r.ReadInt64(), r.ReadInt32());
            default:
                throw new InvalidDataException($"unknown frame type {type}");
        }
    }

    private static (int From, int To, long Term) ReadHeader(BinaryReader r) => (r.ReadInt32(), r.ReadInt32(), r.ReadInt64());

    /// <summary>Reads one frame, or returns null at a clean end of stream.</summary>
    public static async Task<object?> ReadAsync(Stream stream, CancellationToken ct)
    {
        var lenBuf = new byte[4];
        if (!await ReadExactly(stream, lenBuf, ct)) return null;
        int len = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);
        if (len <= 0 || len > MaxFrame) throw new InvalidDataException($"bad frame length {len}");
        var body = new byte[len];
        if (!await ReadExactly(stream, body, ct)) throw new EndOfStreamException("connection closed mid-frame");
        return Decode(body);
    }

    private static async Task<bool> ReadExactly(Stream s, byte[] buffer, CancellationToken ct)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = await s.ReadAsync(buffer.AsMemory(read), ct);
            if (n == 0)
            {
                if (read == 0) return false;
                throw new EndOfStreamException();
            }
            read += n;
        }
        return true;
    }
}
