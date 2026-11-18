namespace Lattice.Util;

/// <summary>Non-cryptographic hashes used for digests, trace fingerprints and checksums.</summary>
public static class Hashing
{
    private const ulong FnvOffset = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    public static ulong Fnv64(ReadOnlySpan<byte> data)
    {
        ulong h = FnvOffset;
        foreach (byte b in data) { h ^= b; h *= FnvPrime; }
        return h;
    }

    /// <summary>Folds a 64-bit value into a running hash (used for the simulator trace fingerprint).</summary>
    public static ulong Mix(ulong h, ulong value)
    {
        h ^= value + 0x9E3779B97F4A7C15UL + (h << 6) + (h >> 2);
        return h * FnvPrime;
    }

    private static readonly uint[] CrcTable = BuildCrcTable();

    private static uint[] BuildCrcTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    /// <summary>CRC-32 (IEEE), used to detect torn or corrupted records on disk.</summary>
    public static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint c = 0xFFFFFFFFu;
        foreach (byte b in data) c = CrcTable[(c ^ b) & 0xFF] ^ (c >> 8);
        return c ^ 0xFFFFFFFFu;
    }
}
