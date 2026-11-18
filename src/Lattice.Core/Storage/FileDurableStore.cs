using System.Buffers.Binary;
using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Storage;

/// <summary>
/// Raft's durable state in a directory of three files:
/// <list type="bullet">
/// <item><c>hardstate</c>: term and vote with a CRC, replaced atomically (write temp file, fsync, rename).</item>
/// <item><c>snapshot</c>: the latest snapshot with a CRC, replaced the same way.</item>
/// <item><c>log</c>: a header (magic, base index) followed by append-only records
/// <c>[length][crc][kind][payload]</c>, where kind is an entry or a truncation marker.</item>
/// </list>
/// Appends are buffered and made durable by <see cref="Sync"/> (FileStream.Flush(true), which is
/// FlushFileBuffers on Windows and fsync elsewhere). On load, a torn or corrupt record ends the log
/// and the file is cut back to the last good record. Saving a snapshot rewrites the log without the
/// compacted prefix.
/// </summary>
public sealed class FileDurableStore : IDurableStore, IDisposable
{
    private const ulong Magic = 0x31474F4C5454414CUL; // "LATTLOG1"
    private const byte KindEntry = 1;
    private const byte KindTruncate = 2;

    private readonly string _dir;
    private FileStream? _logFile;
    private RaftLog _mirror = new();
    private long _term;
    private int _vote;
    private bool _hardStateDirty;

    public FileDurableStore(string directory)
    {
        _dir = directory;
        Directory.CreateDirectory(directory);
    }

    private string HardStatePath => Path.Combine(_dir, "hardstate");
    private string SnapshotPath => Path.Combine(_dir, "snapshot");
    private string LogPath => Path.Combine(_dir, "log");

    public PersistentState Load()
    {
        _logFile?.Dispose();
        (_term, _vote) = ReadHardState();
        var snapshot = ReadSnapshot();
        var (baseIndex, entries, validLength) = ReadLog();

        if (baseIndex > snapshot.LastIndex)
            throw new InvalidDataException($"log starts after index {baseIndex} but the snapshot only covers {snapshot.LastIndex}");

        // Rebuild the log on top of the snapshot. If the log was not yet rewritten after the snapshot
        // was saved, skip the entries the snapshot already covers (same rule as RaftLog.CompactTo).
        var log = new RaftLog(snapshot);
        var atSnapshot = entries.FirstOrDefault(e => e.Index == snapshot.LastIndex);
        bool keepSuffix = snapshot.LastIndex == 0 || atSnapshot is null && baseIndex == snapshot.LastIndex
                          || atSnapshot is not null && atSnapshot.Term == snapshot.LastTerm;
        if (keepSuffix)
            foreach (var e in entries)
                if (e.Index > snapshot.LastIndex) log.Append(e);
        _mirror = log.Clone();

        if (!File.Exists(LogPath))
        {
            RewriteLog();
        }
        else
        {
            _logFile = new FileStream(LogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 1 << 16);
            if (_logFile.Length != validLength) _logFile.SetLength(validLength); // drop a torn tail
            _logFile.Seek(0, SeekOrigin.End);
            if (baseIndex != snapshot.LastIndex || !keepSuffix) RewriteLog();
        }
        return new PersistentState(_term, _vote, log);
    }

    public void SetHardState(long term, int votedFor)
    {
        _term = term;
        _vote = votedFor;
        _hardStateDirty = true;
    }

    public void Append(LogEntry entry)
    {
        if (entry.Index <= _mirror.LastIndex) _mirror.TruncateFrom(entry.Index);
        _mirror.Append(entry);
        var payload = new byte[1 + 8 + 8 + entry.Data.Length];
        payload[0] = KindEntry;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1), entry.Term);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(9), entry.Index);
        entry.Data.CopyTo(payload, 17);
        WriteRecord(payload);
    }

    public void TruncateFrom(long index)
    {
        _mirror.TruncateFrom(index);
        var payload = new byte[9];
        payload[0] = KindTruncate;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1), index);
        WriteRecord(payload);
    }

    public void SaveSnapshot(Snapshot snapshot)
    {
        // 1. The snapshot becomes durable first. 2. Then the log is rewritten without the covered prefix.
        // A crash between the two leaves an old log under a new snapshot, which Load handles.
        var body = new byte[16 + snapshot.Data.Length];
        BinaryPrimitives.WriteInt64LittleEndian(body, snapshot.LastIndex);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), snapshot.LastTerm);
        snapshot.Data.CopyTo(body, 16);
        WriteFileAtomically(SnapshotPath, WithCrc(body));
        _mirror.CompactTo(snapshot);
        RewriteLog();
    }

    public void Sync()
    {
        _logFile?.Flush(flushToDisk: true);
        if (_hardStateDirty)
        {
            var body = new byte[12];
            BinaryPrimitives.WriteInt64LittleEndian(body, _term);
            BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(8), _vote);
            WriteFileAtomically(HardStatePath, WithCrc(body));
            _hardStateDirty = false;
        }
    }

    public void Dispose()
    {
        _logFile?.Dispose();
        _logFile = null;
    }

    // ------------------------------------------------------------------ helpers

    private void WriteRecord(byte[] payload)
    {
        if (_logFile is null) throw new InvalidOperationException("Load must be called first");
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], Hashing.Crc32(payload));
        _logFile.Write(header);
        _logFile.Write(payload);
    }

    private void RewriteLog()
    {
        string tmp = LogPath + ".tmp";
        using (var f = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16))
        {
            Span<byte> header = stackalloc byte[16];
            BinaryPrimitives.WriteUInt64LittleEndian(header, Magic);
            BinaryPrimitives.WriteInt64LittleEndian(header[8..], _mirror.SnapshotIndex);
            f.Write(header);
            _logFile?.Dispose();
            _logFile = f; // borrow WriteRecord
            foreach (var e in _mirror.Entries.ToArray())
            {
                var payload = new byte[17 + e.Data.Length];
                payload[0] = KindEntry;
                BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1), e.Term);
                BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(9), e.Index);
                e.Data.CopyTo(payload, 17);
                WriteRecord(payload);
            }
            f.Flush(flushToDisk: true);
            _logFile = null;
        }
        File.Move(tmp, LogPath, overwrite: true);
        _logFile = new FileStream(LogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 1 << 16);
        _logFile.Seek(0, SeekOrigin.End);
    }

    private (long Term, int Vote) ReadHardState()
    {
        if (!File.Exists(HardStatePath)) return (0, 0);
        var body = CheckCrc(File.ReadAllBytes(HardStatePath), "hardstate");
        return (BinaryPrimitives.ReadInt64LittleEndian(body), BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(8)));
    }

    private Snapshot ReadSnapshot()
    {
        if (!File.Exists(SnapshotPath)) return Snapshot.Empty;
        var body = CheckCrc(File.ReadAllBytes(SnapshotPath), "snapshot");
        return new Snapshot(BinaryPrimitives.ReadInt64LittleEndian(body),
            BinaryPrimitives.ReadInt64LittleEndian(body.AsSpan(8)), body[16..]);
    }

    /// <summary>Replays the log file. Returns its base index, the surviving entries and the length of the valid prefix.</summary>
    private (long BaseIndex, List<LogEntry> Entries, long ValidLength) ReadLog()
    {
        var entries = new List<LogEntry>();
        if (!File.Exists(LogPath)) return (0, entries, 0);
        byte[] all = File.ReadAllBytes(LogPath);
        if (all.Length < 16 || BinaryPrimitives.ReadUInt64LittleEndian(all) != Magic)
            throw new InvalidDataException("log file has a bad header");
        long baseIndex = BinaryPrimitives.ReadInt64LittleEndian(all.AsSpan(8));
        long pos = 16;
        while (pos + 8 <= all.Length)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(all.AsSpan((int)pos));
            uint crc = BinaryPrimitives.ReadUInt32LittleEndian(all.AsSpan((int)pos + 4));
            if (len <= 0 || pos + 8 + len > all.Length) break;
            var payload = all.AsSpan((int)pos + 8, len);
            if (Hashing.Crc32(payload) != crc) break;
            if (payload[0] == KindEntry)
            {
                long term = BinaryPrimitives.ReadInt64LittleEndian(payload[1..]);
                long index = BinaryPrimitives.ReadInt64LittleEndian(payload[9..]);
                long expected = entries.Count == 0 ? baseIndex + 1 : entries[^1].Index + 1;
                if (index < expected) entries.RemoveAll(e => e.Index >= index);
                else if (index > expected) break;
                entries.Add(new LogEntry(term, index, payload[17..].ToArray()));
            }
            else if (payload[0] == KindTruncate)
            {
                long index = BinaryPrimitives.ReadInt64LittleEndian(payload[1..]);
                entries.RemoveAll(e => e.Index >= index);
            }
            else break;
            pos += 8 + len;
        }
        return (baseIndex, entries, pos);
    }

    private static byte[] WithCrc(byte[] body)
    {
        var all = new byte[body.Length + 4];
        body.CopyTo(all, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(all.AsSpan(body.Length), Hashing.Crc32(body));
        return all;
    }

    private static byte[] CheckCrc(byte[] all, string what)
    {
        if (all.Length < 4) throw new InvalidDataException($"{what} file is too short");
        var body = all[..^4];
        if (Hashing.Crc32(body) != BinaryPrimitives.ReadUInt32LittleEndian(all.AsSpan(all.Length - 4)))
            throw new InvalidDataException($"{what} file failed its checksum");
        return body;
    }

    private static void WriteFileAtomically(string path, byte[] content)
    {
        string tmp = path + ".tmp";
        using (var f = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            f.Write(content);
            f.Flush(flushToDisk: true);
        }
        File.Move(tmp, path, overwrite: true);
    }
}
