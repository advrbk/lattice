using System.Diagnostics;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Cli;

internal static class ChaosCommands
{
    /// <summary>
    /// lattice chaos --seeds N [--start S] [--nodes 3|5] [--bug name] [--parallel P] [--no-invariants] [--stop]
    /// lattice chaos --seed S [--nodes n] [--bug name] [--faults 0,3|none] [--trace] [--shrink]
    /// </summary>
    public static int Chaos(Args a)
    {
        int? nodes = a.Get("nodes") is { } n ? int.Parse(n) : null;
        var bugs = a.Get("bug") is { } b ? BugNames.Parse(b) : InjectedBugs.None;
        bool invariants = !a.Flag("no-invariants");

        if (a.Get("seed") is { } single)
        {
            ulong seed = ulong.Parse(single);
            bool trace = a.Flag("trace");
            bool shrink = a.Flag("shrink");
            string? faultList = a.Get("faults");
            a.RejectUnknown();
            return RunOne(seed, nodes, bugs, invariants, faultList, trace, shrink);
        }

        var options = new ChaosOptions
        {
            FirstSeed = a.ULong("start", 1),
            Seeds = a.Int("seeds", 1000),
            Nodes = nodes,
            Bugs = bugs,
            CheckInvariants = invariants,
            Parallelism = a.Int("parallel", 2),
            StopOnFirstFailure = a.Flag("stop"),
        };
        a.RejectUnknown();

        Console.WriteLine($"Chaos campaign: seeds {options.FirstSeed}..{options.FirstSeed + (ulong)options.Seeds - 1}, " +
                          $"nodes {(nodes?.ToString() ?? "3 or 5 per seed")}, bugs {(bugs == InjectedBugs.None ? "none" : BugNames.Format(bugs))}, " +
                          $"invariants {(invariants ? "on" : "off")}, parallelism {options.Parallelism}");
        int step = Math.Max(1, options.Seeds / 20);
        var summary = ChaosRunner.Run(options, (done, s) =>
        {
            if (done % step == 0 || done == options.Seeds)
                Console.WriteLine($"  {done,6} / {options.Seeds} seeds, {s.Failures} failing");
        });
        PrintSummary(summary, options);
        return summary.Failures == 0 ? 0 : 1;
    }

    private static void PrintSummary(ChaosSummary s, ChaosOptions o)
    {
        Console.WriteLine();
        Console.WriteLine($"Seeds run:              {s.SeedsRun:N0}");
        Console.WriteLine($"Failing seeds:          {s.Failures:N0}");
        foreach (var (kind, count) in s.FailuresByKind.OrderByDescending(k => k.Value))
        {
            var examples = s.FailedRuns.Where(r => r.FailureKind == kind).Select(r => r.Seed).Take(5);
            Console.WriteLine($"  {kind,-22}{count,8:N0}   e.g. seeds {string.Join(", ", examples)}");
        }
        Console.WriteLine($"Simulated time:         {s.SimMicros / 1e6 / 3600:N2} hours ({s.SimMicros / 1e6:N0} s)");
        Console.WriteLine($"Events processed:       {s.Events:N0}");
        Console.WriteLine($"Client operations:      {s.Operations:N0} ({s.OpsCompleted:N0} answered, {s.OpsUnknown:N0} unknown outcome)");
        Console.WriteLine($"Faults scheduled:       {s.Faults:N0} ({s.FaultsFired:N0} took effect; the rest found nothing to act on)");
        Console.WriteLine($"Leader elections:       {s.Elections:N0}");
        Console.WriteLine($"Entries committed:      {s.Commits:N0}");
        Console.WriteLine($"Messages sent/dropped:  {s.MessagesSent:N0} / {s.MessagesDropped:N0} ({s.SnapshotsSent:N0} InstallSnapshot)");
        Console.WriteLine($"Checker search steps:   {s.CheckerSteps:N0}");
        Console.WriteLine($"Wall time:              {s.WallSeconds:N1} s ({s.SeedsRun / s.WallSeconds:N1} seeds/s with parallelism {o.Parallelism})");
        if (s.FailedRuns.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Failures (first 10):");
            foreach (var r in s.FailedRuns.Take(10))
            {
                Console.WriteLine($"  seed {r.Seed}: {r.FailureKind}");
                Console.WriteLine($"    {FirstLine(r.Failure)}");
                Console.WriteLine($"    repro: {ChaosRunner.ReproCommand(r.Seed, o.Nodes, o.Bugs, o.CheckInvariants)}");
            }
            Console.WriteLine("  Add --shrink to a repro command to search for a smaller fault schedule.");
        }
    }

    private static int RunOne(ulong seed, int? nodes, InjectedBugs bugs, bool invariants, string? faultList, bool trace, bool shrink)
    {
        var options = SimOptions.ForSeed(seed, nodes, bugs, checkInvariants: invariants);
        var plan = FaultPlan.Generate(seed, options);
        IReadOnlyList<Fault> faults = plan;
        if (faultList is not null)
        {
            var keep = faultList == "none" ? new HashSet<int>() : faultList.Split(',').Select(int.Parse).ToHashSet();
            faults = plan.Where(f => keep.Contains(f.Index)).ToList();
        }

        Console.WriteLine($"Seed {seed}: {options}");
        Console.WriteLine($"Faults ({faults.Count} of {plan.Count}):");
        foreach (var f in faults) Console.WriteLine($"  {f}");
        Console.WriteLine();

        var sw = Stopwatch.StartNew();
        var result = new Simulation(seed, options, faults, trace ? Console.Out : null).Run();
        sw.Stop();
        Console.WriteLine();
        Console.WriteLine($"Faults that took effect: {result.FaultsFired} of {faults.Count}, crashes {result.Crashes}, snapshots sent {result.SnapshotsSent}");
        Console.WriteLine($"Events {result.Events:N0}, simulated {result.SimMicros / 1e6:0.0} s in {sw.ElapsedMilliseconds} ms, " +
                          $"operations {result.OpsCompleted} answered / {result.OpsUnknown} unknown, elections {result.Elections}, " +
                          $"committed {result.Commits}, trace hash {result.TraceHash:x16}");
        if (result.Passed)
        {
            Console.WriteLine(invariants ? "PASS: the Raft invariants held and the history is linearizable." : "PASS: the history is linearizable (invariant checks were off).");
            return 0;
        }

        Console.WriteLine($"FAIL ({result.FailureKind}):");
        Console.WriteLine(result.Failure);
        if (shrink)
        {
            Console.WriteLine("Shrinking the fault schedule...");
            var (smaller, shrunk) = ChaosRunner.Shrink(seed, nodes, bugs, invariants);
            Console.WriteLine($"Still fails ({shrunk.FailureKind}) with {smaller.Count} of {plan.Count} faults:");
            foreach (var f in smaller) Console.WriteLine($"  {f}");
            Console.WriteLine($"Minimal repro: {ChaosRunner.ReproCommand(seed, nodes, bugs, invariants, smaller)}");
        }
        else
        {
            Console.WriteLine($"Repro: {ChaosRunner.ReproCommand(seed, nodes, bugs, invariants, faultList is null ? null : faults)}");
        }
        return 1;
    }

    /// <summary>
    /// lattice bugs [--seeds N] [--parallel P]: for each injected bug, how many seeds until the chaos test
    /// catches it, with and without the Raft invariant checks (the second column is the linearizability
    /// checker on its own).
    /// </summary>
    public static int Bugs(Args a)
    {
        int seeds = a.Int("seeds", 2000);
        int parallel = a.Int("parallel", 2);
        ulong start = a.ULong("start", 1);
        string? only = a.Get("only");
        a.RejectUnknown();
        var bugs = only is null ? BugNames.All : BugNames.All.Where(b => b.Name == only).ToArray();
        if (bugs.Length == 0) throw new ArgumentException($"unknown bug '{only}'");

        Console.WriteLine($"Bug injection: each bug alone, seeds {start}..{start + (ulong)seeds - 1}, parallelism {parallel}");
        Console.WriteLine();
        var rows = new List<string[]>();
        foreach (var (name, bug, _) in bugs)
        {
            foreach (bool invariants in new[] { true, false })
            {
                var sw = Stopwatch.StartNew();
                var s = ChaosRunner.Run(new ChaosOptions
                {
                    FirstSeed = start, Seeds = seeds, Bugs = bug, CheckInvariants = invariants, Parallelism = parallel,
                });
                sw.Stop();
                var first = s.FailedRuns.FirstOrDefault();
                string kinds = string.Join(", ", s.FailuresByKind.OrderByDescending(k => k.Value).Select(k => $"{k.Key} {k.Value}"));
                rows.Add(new[]
                {
                    name, invariants ? "invariants + checker" : "checker only",
                    s.FirstFailureAfter == 0 ? $"not caught in {seeds}" : s.FirstFailureAfter.ToString(),
                    $"{s.Failures}/{s.SeedsRun}",
                    kinds.Length == 0 ? "-" : kinds,
                    first is null ? "-" : first.Seed.ToString(),
                    $"{sw.Elapsed.TotalSeconds:0.0}",
                });
                Console.WriteLine($"  {name,-18} {(invariants ? "invariants + checker" : "checker only"),-21} first caught at seed #{(s.FirstFailureAfter == 0 ? "-" : s.FirstFailureAfter.ToString()),-5} failing {s.Failures}/{s.SeedsRun}  {kinds}");
            }
        }
        Console.WriteLine();
        Console.WriteLine("| Bug | Detectors | Seeds until first catch | Failing seeds | Caught by | First failing seed | Wall s |");
        Console.WriteLine("|---|---|---:|---:|---|---:|---:|");
        foreach (var r in rows) Console.WriteLine($"| {string.Join(" | ", r)} |");
        return 0;
    }

    private static string FirstLine(string? s) => s is null ? "" : s.Split('\n')[0].Trim();
}
