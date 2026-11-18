using System.Numerics;

namespace Lattice.Util;

/// <summary>
/// A small seeded random number generator (xoshiro256** seeded through SplitMix64).
/// System.Random is not used for the simulator because its seeded sequence is an
/// implementation detail of the runtime; this one produces the same numbers on every
/// machine and .NET version, which is what makes a chaos seed replayable.
/// </summary>
public sealed class DetRandom
{
    private ulong _s0, _s1, _s2, _s3;

    public DetRandom(ulong seed)
    {
        ulong x = seed;
        _s0 = SplitMix(ref x);
        _s1 = SplitMix(ref x);
        _s2 = SplitMix(ref x);
        _s3 = SplitMix(ref x);
    }

    private static ulong SplitMix(ref ulong x)
    {
        x += 0x9E3779B97F4A7C15UL;
        ulong z = x;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    public ulong NextU64()
    {
        ulong result = BitOperations.RotateLeft(_s1 * 5, 7) * 9;
        ulong t = _s1 << 17;
        _s2 ^= _s0;
        _s3 ^= _s1;
        _s1 ^= _s2;
        _s0 ^= _s3;
        _s2 ^= t;
        _s3 = BitOperations.RotateLeft(_s3, 45);
        return result;
    }

    /// <summary>Uniform integer in [0, maxExclusive).</summary>
    public int Next(int maxExclusive)
    {
        if (maxExclusive <= 0) throw new ArgumentOutOfRangeException(nameof(maxExclusive));
        return (int)(NextU64() % (ulong)maxExclusive);
    }

    /// <summary>Uniform integer in [min, maxExclusive).</summary>
    public int Next(int min, int maxExclusive) => min + Next(maxExclusive - min);

    /// <summary>Uniform long in [min, maxExclusive).</summary>
    public long NextLong(long min, long maxExclusive)
    {
        if (maxExclusive <= min) throw new ArgumentOutOfRangeException(nameof(maxExclusive));
        return min + (long)(NextU64() % (ulong)(maxExclusive - min));
    }

    /// <summary>Uniform double in [0, 1).</summary>
    public double NextDouble() => (NextU64() >> 11) * (1.0 / (1UL << 53));

    public bool Chance(double p) => p > 0 && NextDouble() < p;

    /// <summary>A new generator whose stream is independent of this one.</summary>
    public DetRandom Fork() => new(NextU64());
}
