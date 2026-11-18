using Lattice.Kv;
using Lattice.Raft;
using Lattice.Util;

namespace Lattice.Sim;

/// <summary>Everything that shapes one simulated run. All times are in simulated microseconds.</summary>
public sealed class SimOptions
{
    public int Nodes { get; init; } = 3;
    public int Clients { get; init; } = 4;
    public int Keys { get; init; } = 3;

    /// <summary>Clients start operations and faults fire only during the workload.</summary>
    public long WorkloadMicros { get; init; } = 5_000_000;
    /// <summary>After the workload every fault is healed and clients finish what they started.</summary>
    public long QuietMicros { get; init; } = 3_000_000;

    public long TickMicros { get; init; } = 10_000;
    /// <summary>Each node's clock runs up to this fraction fast or slow (chosen per incarnation).</summary>
    public double MaxClockDrift { get; init; } = 0.1;
    /// <summary>Random jitter added to every tick.</summary>
    public long TickJitterMicros { get; init; } = 2_000;

    public long MinDelayMicros { get; init; } = 500;
    public long MaxDelayMicros { get; init; } = 5_000;
    public double DropRate { get; init; }
    public double DuplicateRate { get; init; }
    /// <summary>Chance that a message gets an extra long delay, so it overtakes nothing and is overtaken by many.</summary>
    public double ReorderRate { get; init; }
    public long ReorderMaxMicros { get; init; } = 60_000;

    /// <summary>How often the simulated OS writes buffered disk writes back on its own.</summary>
    public long WritebackMinMicros { get; init; } = 20_000;
    public long WritebackMaxMicros { get; init; } = 300_000;

    /// <summary>A client gives up on an operation after this long and records its outcome as unknown.</summary>
    public long OperationTimeoutMicros { get; init; } = 2_000_000;

    public bool Faults { get; init; } = true;
    public bool CheckInvariants { get; init; } = true;
    public ReadMode ReadMode { get; init; } = ReadMode.ReadIndex;
    public RaftOptions Raft { get; init; } = new();

    public InjectedBugs Bugs => Raft.Bugs;

    /// <summary>
    /// Derives a random but reproducible configuration from a seed: cluster size, network quality,
    /// batching and snapshot settings, read mode and check-quorum all vary between seeds.
    /// </summary>
    public static SimOptions ForSeed(ulong seed, int? nodes = null, InjectedBugs bugs = InjectedBugs.None,
        bool faults = true, bool checkInvariants = true)
    {
        var rng = new DetRandom(seed ^ 0x5EEDC0FFEE12345UL);
        int n = nodes ?? (rng.Chance(0.5) ? 3 : 5);
        int[] batches = { 1, 2, 4, 16, 64 };
        int[] thresholds = { 8, 32, 128, 100_000 };
        var readMode = rng.Chance(0.75) ? ReadMode.ReadIndex : ReadMode.Log;
        if ((bugs & InjectedBugs.StaleRead) != 0) readMode = ReadMode.ReadIndex; // the bug lives on that path
        return new SimOptions
        {
            Nodes = n,
            Clients = rng.Next(3, 7),
            Keys = rng.Next(1, 5),
            MinDelayMicros = 200,
            MaxDelayMicros = rng.NextLong(1_000, 10_000),
            DropRate = rng.Chance(0.3) ? 0 : rng.NextDouble() * 0.05,
            DuplicateRate = rng.NextDouble() * 0.03,
            ReorderRate = rng.NextDouble() * 0.05,
            MaxClockDrift = rng.NextDouble() * 0.2,
            Faults = faults,
            CheckInvariants = checkInvariants,
            ReadMode = readMode,
            Raft = new RaftOptions
            {
                ElectionTimeoutMinTicks = 15,
                ElectionTimeoutMaxTicks = 30,
                HeartbeatTicks = 3,
                MaxEntriesPerMessage = batches[rng.Next(batches.Length)],
                MaxInflightEntries = rng.Chance(0.5) ? 8 : 256,
                SnapshotThreshold = thresholds[rng.Next(thresholds.Length)],
                CheckQuorum = rng.Chance(0.7),
                Bugs = bugs,
            },
        };
    }

    public override string ToString() =>
        $"nodes={Nodes} clients={Clients} keys={Keys} delay={MinDelayMicros / 1000.0:0.#}-{MaxDelayMicros / 1000.0:0.#}ms " +
        $"drop={DropRate:P1} dup={DuplicateRate:P1} reorder={ReorderRate:P1} drift={MaxClockDrift:P0} " +
        $"batch={Raft.MaxEntriesPerMessage} inflight={Raft.MaxInflightEntries} snapshot={Raft.SnapshotThreshold} " +
        $"checkQuorum={Raft.CheckQuorum} reads={ReadMode}";
}
