using System.Diagnostics;
using Lattice.Raft;

namespace Lattice.Sim;

public sealed class ChaosOptions
{
    public ulong FirstSeed { get; init; } = 1;
    public int Seeds { get; init; } = 100;
    public int? Nodes { get; init; }
    public InjectedBugs Bugs { get; init; }
    public bool CheckInvariants { get; init; } = true;
    public int Parallelism { get; init; } = 2;
    public bool StopOnFirstFailure { get; init; }
}

public sealed class ChaosSummary
{
    public int SeedsRun;
    public int Failures;
    public long Events;
    public long SimMicros;
    public long Operations;
    public long OpsCompleted;
    public long OpsUnknown;
    /// <summary>Faults scheduled by the seeds' fault plans.</summary>
    public long Faults;
    /// <summary>Faults that actually took effect (see <see cref="SimResult.FaultsFired"/>).</summary>
    public long FaultsFired;
    public long Elections;
    public long Commits;
    public long MessagesSent;
    public long MessagesDropped;
    public long SnapshotsSent;
    public long CheckerSteps;
    public double WallSeconds;
    public readonly Dictionary<string, int> FailuresByKind = new();
    public readonly List<SimResult> FailedRuns = new();
    /// <summary>Position (1-based) of the first seed with a definite failure (not counting checker give-ups), or 0.</summary>
    public int FirstFailureAfter;
}

/// <summary>Runs many seeds, aggregates the results and builds repro commands for failures.</summary>
public static class ChaosRunner
{
    public static SimResult RunSeed(ulong seed, int? nodes, InjectedBugs bugs, bool checkInvariants = true,
        IReadOnlyList<Fault>? faults = null, TextWriter? trace = null)
    {
        var options = SimOptions.ForSeed(seed, nodes, bugs, checkInvariants: checkInvariants);
        return new Simulation(seed, options, faults, trace).Run();
    }

    public static ChaosSummary Run(ChaosOptions o, Action<int, ChaosSummary>? progress = null)
    {
        var summary = new ChaosSummary();
        var sw = Stopwatch.StartNew();
        var gate = new object();
        int done = 0;
        int firstFailurePos = int.MaxValue;
        using var cts = new CancellationTokenSource();

        try
        {
            Parallel.For(0, o.Seeds, new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, o.Parallelism), CancellationToken = cts.Token }, i =>
            {
                if (o.StopOnFirstFailure && i > Volatile.Read(ref firstFailurePos)) return;
                ulong seed = o.FirstSeed + (ulong)i;
                var r = RunSeed(seed, o.Nodes, o.Bugs, o.CheckInvariants);
                r.History = new(); // keep memory flat over thousands of runs
                lock (gate)
                {
                    summary.SeedsRun++;
                    summary.Events += r.Events;
                    summary.SimMicros += r.SimMicros;
                    summary.Operations += r.OpsCompleted + r.OpsUnknown;
                    summary.OpsCompleted += r.OpsCompleted;
                    summary.OpsUnknown += r.OpsUnknown;
                    summary.Faults += r.Faults;
                    summary.FaultsFired += r.FaultsFired;
                    summary.Elections += r.Elections;
                    summary.Commits += r.Commits;
                    summary.MessagesSent += r.MessagesSent;
                    summary.MessagesDropped += r.MessagesDropped;
                    summary.SnapshotsSent += r.SnapshotsSent;
                    summary.CheckerSteps += r.CheckerSteps;
                    if (!r.Passed)
                    {
                        summary.Failures++;
                        summary.FailuresByKind[r.FailureKind!] = summary.FailuresByKind.GetValueOrDefault(r.FailureKind!) + 1;
                        // Keep a few examples of every kind of failure.
                        if (summary.FailedRuns.Count(f => f.FailureKind == r.FailureKind) < 10) summary.FailedRuns.Add(r);
                        // An inconclusive check is reported, but it is not proof that a bug was caught.
                        if (r.FailureKind != "CheckerGaveUp" && i < firstFailurePos) firstFailurePos = i;
                    }
                    done++;
                    progress?.Invoke(done, summary);
                }
            });
        }
        catch (OperationCanceledException) { }

        summary.FailedRuns.Sort((a, b) => a.Seed.CompareTo(b.Seed));
        summary.FirstFailureAfter = firstFailurePos == int.MaxValue ? 0 : firstFailurePos + 1;
        summary.WallSeconds = sw.Elapsed.TotalSeconds;
        return summary;
    }

    /// <summary>
    /// Greedy shrinking: try dropping each fault in turn and keep the drop if the run still fails.
    /// Returns the smallest fault list found that still reproduces a failure.
    /// </summary>
    public static (List<Fault> Faults, SimResult Result) Shrink(ulong seed, int? nodes, InjectedBugs bugs, bool checkInvariants, int maxRuns = 200)
    {
        var options = SimOptions.ForSeed(seed, nodes, bugs, checkInvariants: checkInvariants);
        var current = FaultPlan.Generate(seed, options);
        var best = new Simulation(seed, options, current).Run();
        if (best.Passed) return (current, best);
        int runs = 0;
        bool progress = true;
        while (progress && runs < maxRuns)
        {
            progress = false;
            for (int i = current.Count - 1; i >= 0 && runs < maxRuns; i--)
            {
                var candidate = current.Where((_, k) => k != i).ToList();
                var r = new Simulation(seed, options, candidate).Run();
                runs++;
                if (!r.Passed)
                {
                    current = candidate;
                    best = r;
                    progress = true;
                }
            }
        }
        return (current, best);
    }

    public static string ReproCommand(ulong seed, int? nodes, InjectedBugs bugs, bool checkInvariants, IReadOnlyList<Fault>? faults = null)
    {
        var parts = new List<string> { "lattice chaos", $"--seed {seed}" };
        if (nodes is not null) parts.Add($"--nodes {nodes}");
        if (bugs != InjectedBugs.None) parts.Add($"--bug {BugNames.Format(bugs)}");
        if (!checkInvariants) parts.Add("--no-invariants");
        if (faults is not null) parts.Add($"--faults {(faults.Count == 0 ? "none" : string.Join(",", faults.Select(f => f.Index)))}");
        parts.Add("--trace");
        return string.Join(" ", parts);
    }
}

public static class BugNames
{
    public static readonly (string Name, InjectedBugs Bug, string Description)[] All =
    {
        ("vote-no-log-check", InjectedBugs.VoteWithoutLogCheck, "grant votes without the log up-to-date check"),
        ("commit-old-term", InjectedBugs.CommitPreviousTerm, "commit earlier-term entries by counting replicas"),
        ("skip-fsync", InjectedBugs.SkipFsync, "reply before fsyncing term, vote and log"),
        ("stale-read", InjectedBugs.StaleRead, "serve reads without confirming leadership"),
        ("no-dedupe", InjectedBugs.NoDedupe, "apply retried commands again (no client sessions)"),
    };

    public static InjectedBugs Parse(string text)
    {
        var result = InjectedBugs.None;
        foreach (var part in text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var match = All.FirstOrDefault(b => b.Name == part);
            if (match.Name is null)
                throw new ArgumentException($"unknown bug '{part}'. Known: {string.Join(", ", All.Select(b => b.Name))}");
            result |= match.Bug;
        }
        return result;
    }

    public static string Format(InjectedBugs bugs) =>
        string.Join(",", All.Where(b => (bugs & b.Bug) != 0).Select(b => b.Name));
}
