using Lattice.Util;

namespace Lattice.Sim;

public enum FaultKind
{
    /// <summary>Split the servers into two groups that cannot talk to each other.</summary>
    Partition,
    /// <summary>Cut whoever is leader at that moment off from everyone else.</summary>
    IsolateLeader,
    /// <summary>Crash a server (losing unsynced disk writes) and restart it later.</summary>
    Crash,
    /// <summary>Crash whoever is leader at that moment and restart it later.</summary>
    KillLeader,
    /// <summary>Crash the next node to win an election, a few milliseconds after it wins (before its first entries spread).</summary>
    KillNewLeader,
    /// <summary>Freeze a server (a long GC pause or a stopped VM): its clock and inbox stop, then resume.</summary>
    Pause,
    /// <summary>Freeze whoever is leader at that moment.</summary>
    PauseLeader,
    /// <summary>Extra message loss for a while.</summary>
    LossBurst,
    /// <summary>Extra message duplication for a while.</summary>
    DuplicateBurst,
    /// <summary>Extra latency for a while.</summary>
    SlowNetwork,
}

/// <summary>One scheduled fault. Each fault undoes itself after <see cref="DurationMicros"/>.</summary>
public sealed record Fault(int Index, FaultKind Kind, long AtMicros, long DurationMicros, int Target, int[] Group, double Amount)
{
    public override string ToString()
    {
        string what = Kind switch
        {
            FaultKind.Partition => $"partition {{{string.Join(",", Group)}}} from the rest",
            FaultKind.Crash => $"crash node {Target}",
            FaultKind.Pause => $"pause node {Target}",
            FaultKind.LossBurst => $"drop {Amount:P0} of messages",
            FaultKind.DuplicateBurst => $"duplicate {Amount:P0} of messages",
            FaultKind.SlowNetwork => $"add up to {Amount / 1000:0} ms latency",
            FaultKind.KillNewLeader => $"crash the next new leader {Amount / 1000:0.0} ms after it wins",
            _ => Kind.ToString(),
        };
        return $"[{Index}] at {AtMicros / 1000.0:0.0} ms for {DurationMicros / 1000.0:0.0} ms: {what}";
    }
}

public static class FaultPlan
{
    /// <summary>
    /// The fault schedule for a seed. It uses its own random stream, so the schedule does not change
    /// when the rest of the run does (that is what lets the shrinker drop faults one at a time).
    /// </summary>
    public static List<Fault> Generate(ulong seed, SimOptions o)
    {
        var faults = new List<Fault>();
        if (!o.Faults) return faults;
        var rng = new DetRandom(seed * 0x9E3779B97F4A7C15UL + 0xFA017);
        int count = rng.Next(8, 31);
        for (int i = 0; i < count; i++)
        {
            long at = rng.NextLong(200_000, o.WorkloadMicros);
            int roll = rng.Next(100);
            Fault f = roll switch
            {
                < 13 => new Fault(0, FaultKind.Partition, at, rng.NextLong(100_000, 2_000_000), 0, RandomMinority(rng, o.Nodes), 0),
                < 24 => new Fault(0, FaultKind.IsolateLeader, at, rng.NextLong(100_000, 2_000_000), 0, Array.Empty<int>(), 0),
                < 35 => new Fault(0, FaultKind.Crash, at, rng.NextLong(20_000, 1_500_000), rng.Next(1, o.Nodes + 1), Array.Empty<int>(), 0),
                < 46 => new Fault(0, FaultKind.KillLeader, at, rng.NextLong(20_000, 1_500_000), 0, Array.Empty<int>(), 0),
                < 57 => new Fault(0, FaultKind.KillNewLeader, at, rng.NextLong(20_000, 1_000_000), 0, Array.Empty<int>(), rng.NextLong(0, 20_000)),
                < 64 => new Fault(0, FaultKind.Pause, at, rng.NextLong(50_000, 1_000_000), rng.Next(1, o.Nodes + 1), Array.Empty<int>(), 0),
                < 72 => new Fault(0, FaultKind.PauseLeader, at, rng.NextLong(50_000, 1_000_000), 0, Array.Empty<int>(), 0),
                < 83 => new Fault(0, FaultKind.LossBurst, at, rng.NextLong(100_000, 1_500_000), 0, Array.Empty<int>(), 0.05 + rng.NextDouble() * 0.45),
                < 91 => new Fault(0, FaultKind.DuplicateBurst, at, rng.NextLong(100_000, 1_500_000), 0, Array.Empty<int>(), 0.05 + rng.NextDouble() * 0.3),
                _ => new Fault(0, FaultKind.SlowNetwork, at, rng.NextLong(100_000, 1_500_000), 0, Array.Empty<int>(), rng.NextLong(5_000, 80_000)),
            };
            faults.Add(f);
        }
        faults.Sort((a, b) => a.AtMicros.CompareTo(b.AtMicros));
        for (int i = 0; i < faults.Count; i++) faults[i] = faults[i] with { Index = i };
        return faults;
    }

    private static int[] RandomMinority(DetRandom rng, int nodes)
    {
        int size = rng.Next(1, nodes / 2 + 1);
        var all = Enumerable.Range(1, nodes).ToList();
        var group = new List<int>();
        for (int i = 0; i < size; i++)
        {
            int k = rng.Next(all.Count);
            group.Add(all[k]);
            all.RemoveAt(k);
        }
        group.Sort();
        return group.ToArray();
    }
}
