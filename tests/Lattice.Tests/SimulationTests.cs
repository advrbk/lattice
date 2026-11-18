using Lattice.Check;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Tests;

public class SimulationTests
{
    private static string Describe(List<Operation> history) => string.Join("\n", history.Select(o => o.ToString()));

    [Fact]
    public void Same_seed_gives_exactly_the_same_run()
    {
        var a = ChaosRunner.RunSeed(42, null, InjectedBugs.None);
        var b = ChaosRunner.RunSeed(42, null, InjectedBugs.None);
        Assert.Equal(a.TraceHash, b.TraceHash);
        Assert.Equal(a.Events, b.Events);
        Assert.Equal(Describe(a.History), Describe(b.History));

        var c = ChaosRunner.RunSeed(43, null, InjectedBugs.None);
        Assert.NotEqual(a.TraceHash, c.TraceHash);
    }

    [Fact]
    public void Same_seed_gives_the_same_failure()
    {
        var a = ChaosRunner.RunSeed(3, null, InjectedBugs.NoDedupe);
        var b = ChaosRunner.RunSeed(3, null, InjectedBugs.NoDedupe);
        Assert.False(a.Passed);
        Assert.Equal(a.Failure, b.Failure);
        Assert.Equal(a.TraceHash, b.TraceHash);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public void Without_faults_every_operation_completes(int nodes)
    {
        var options = new SimOptions { Nodes = nodes, Clients = 5, Keys = 3, Faults = false };
        var r = new Simulation(1, options).Run();
        Assert.True(r.Passed, r.Failure);
        Assert.Equal(0, r.OpsUnknown);
        Assert.True(r.OpsCompleted > 1000, $"only {r.OpsCompleted} operations completed");
        Assert.Equal(1, r.Elections);
    }

    [Fact]
    public void Lagging_node_catches_up_through_InstallSnapshot()
    {
        var options = new SimOptions
        {
            Nodes = 3, Clients = 4, Keys = 3,
            Raft = new RaftOptions { SnapshotThreshold = 16 },
        };
        // Node 3 is down from 0.5 s to 3.5 s while the others commit and compact far past it.
        var faults = new[] { new Fault(0, FaultKind.Crash, 500_000, 3_000_000, 3, Array.Empty<int>(), 0) };
        var r = new Simulation(5, options, faults).Run();
        Assert.True(r.Passed, r.Failure);
        Assert.True(r.SnapshotsSent > 0, "expected the leader to send a snapshot");
    }

    [Fact]
    public void Leader_partitioned_into_a_minority_cannot_commit_and_clients_recover()
    {
        var options = new SimOptions { Nodes = 5, Clients = 4, Keys = 2, Raft = new RaftOptions { CheckQuorum = false } };
        var faults = new[] { new Fault(0, FaultKind.IsolateLeader, 1_000_000, 2_000_000, 0, Array.Empty<int>(), 0) };
        var r = new Simulation(11, options, faults).Run();
        Assert.True(r.Passed, r.Failure);
        Assert.True(r.Elections >= 2);
    }

    [Fact]
    public void Chaos_campaign_of_2000_seeds_passes()
    {
        var s = ChaosRunner.Run(new ChaosOptions { FirstSeed = 100_000, Seeds = 2000, Parallelism = 2 });
        var first = s.FailedRuns.FirstOrDefault();
        Assert.True(s.Failures == 0, first is null ? "" : $"seed {first.Seed}: {first.Failure}");
        Assert.True(s.Elections > 2000 && s.Faults > 20_000);
    }

    /// <summary>Seeds that once failed on correct code, kept as regression tests (see HOW_IT_WORKS, section 8).</summary>
    [Theory]
    [InlineData(18332UL)] // stale ReadIndex read after a leader restart (stale-term replies echoed Seq)
    public void Regression_seeds_pass(ulong seed)
    {
        var r = ChaosRunner.RunSeed(seed, null, InjectedBugs.None);
        Assert.True(r.Passed, r.Failure);
    }

    [Fact]
    public void Event_limit_and_fault_plan_are_deterministic()
    {
        var o = SimOptions.ForSeed(77);
        var a = FaultPlan.Generate(77, o);
        var b = FaultPlan.Generate(77, SimOptions.ForSeed(77));
        Assert.Equal(a.Select(f => f.ToString()), b.Select(f => f.ToString()));
        Assert.InRange(a.Count, 8, 30);
    }
}

/// <summary>
/// The injected bugs. Each must be caught by the chaos test; seed counts come from `lattice bugs`.
/// </summary>
public class BugInjectionTests
{
    private static ChaosSummary Hunt(InjectedBugs bug, int seeds, bool invariants = true) =>
        ChaosRunner.Run(new ChaosOptions { FirstSeed = 1, Seeds = seeds, Bugs = bug, CheckInvariants = invariants, Parallelism = 2, StopOnFirstFailure = true });

    [Theory]
    [InlineData(InjectedBugs.VoteWithoutLogCheck, 40)]
    [InlineData(InjectedBugs.SkipFsync, 150)]
    [InlineData(InjectedBugs.StaleRead, 150)]
    [InlineData(InjectedBugs.NoDedupe, 20)]
    public void Chaos_test_catches_the_bug(InjectedBugs bug, int seeds)
    {
        var s = Hunt(bug, seeds);
        Assert.True(s.FirstFailureAfter > 0, $"{bug} not caught in {seeds} seeds");
    }

    [Theory]
    [InlineData(InjectedBugs.VoteWithoutLogCheck, 60)]
    [InlineData(InjectedBugs.SkipFsync, 300)]
    [InlineData(InjectedBugs.StaleRead, 150)]
    [InlineData(InjectedBugs.NoDedupe, 20)]
    public void Linearizability_checker_alone_catches_the_bug(InjectedBugs bug, int seeds)
    {
        var s = Hunt(bug, seeds, invariants: false);
        Assert.True(s.FirstFailureAfter > 0, $"{bug} not caught by the checker alone in {seeds} seeds");
        Assert.Contains(s.FailedRuns, r => r.FailureKind is "Linearizability" or "Convergence");
    }

    /// <summary>A seed on which the campaign caught commit-old-term for this version of the code.</summary>
    private const ulong RecordedFigure8Seed = 2508;

    [Fact]
    public void Commit_previous_term_bug_is_caught_by_the_chaos_test()
    {
        // The Figure 8 interleaving is rare under random faults (about one seed in a few thousand), so the
        // search starts at a seed recorded from a campaign. Runs are deterministic, so it fails right away;
        // if a code change shifts the event order, the search simply continues to the next failing seed.
        var s = ChaosRunner.Run(new ChaosOptions
        {
            FirstSeed = RecordedFigure8Seed, Seeds = 20_000, Bugs = InjectedBugs.CommitPreviousTerm,
            Parallelism = 2, StopOnFirstFailure = true,
        });
        Assert.True(s.FirstFailureAfter > 0, "commit-old-term not caught");
        var caught = s.FailedRuns.First(r => r.FailureKind != "CheckerGaveUp");
        Assert.Contains(caught.FailureKind, new[] { "LeaderCompleteness", "StateMachineSafety", "Linearizability" });

        // The same seed passes with the correct commit rule.
        var correct = ChaosRunner.RunSeed(caught.Seed, null, InjectedBugs.None);
        Assert.True(correct.Passed, correct.Failure);
    }

    [Fact]
    public void Shrinking_keeps_the_failure_with_fewer_faults()
    {
        var full = ChaosRunner.RunSeed(2, null, InjectedBugs.VoteWithoutLogCheck);
        Assert.False(full.Passed);
        var (faults, result) = ChaosRunner.Shrink(2, null, InjectedBugs.VoteWithoutLogCheck, checkInvariants: true, maxRuns: 60);
        Assert.False(result.Passed);
        Assert.True(faults.Count <= full.Faults);
        var repro = new Simulation(2, SimOptions.ForSeed(2, bugs: InjectedBugs.VoteWithoutLogCheck), faults).Run();
        Assert.Equal(result.Failure, repro.Failure);
    }
}
