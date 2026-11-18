using Lattice.Kv;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Tests;

/// <summary>
/// Checks that the simulator's faults really happen and that its statistics say what took effect,
/// written during review.
/// </summary>
public class SimulatorHonestyTests
{
    [Fact]
    public void Fault_statistics_count_only_faults_that_took_effect()
    {
        var options = new SimOptions { Nodes = 3, Clients = 3, Keys = 2 };
        var faults = new[]
        {
            new Fault(0, FaultKind.Crash, 500_000, 500_000, 2, Array.Empty<int>(), 0),
            new Fault(1, FaultKind.Crash, 600_000, 100_000, 2, Array.Empty<int>(), 0),   // node 2 is already down
            new Fault(2, FaultKind.DuplicateBurst, 1_500_000, 200_000, 0, Array.Empty<int>(), 0.2),
            new Fault(3, FaultKind.Pause, 2_000_000, 100_000, 3, Array.Empty<int>(), 0),
            new Fault(4, FaultKind.KillNewLeader, 4_990_000, 100_000, 0, Array.Empty<int>(), 1_000), // no election follows
        };
        var r = new Simulation(3, options, faults).Run();
        Assert.True(r.Passed, r.Failure);
        Assert.Equal(5, r.Faults);
        Assert.Equal(3, r.FaultsFired);
        Assert.Equal(1, r.Crashes);
    }

    [Fact]
    public void Network_duplicates_are_really_delivered_twice()
    {
        // With heavy duplication and no client sessions, duplicated client requests and log entries are
        // applied twice and the history stops being linearizable. With sessions it passes.
        SimOptions Options(bool bug) => new()
        {
            Nodes = 3, Clients = 4, Keys = 1, DuplicateRate = 0.3, Faults = false,
            Raft = new RaftOptions { Bugs = bug ? InjectedBugs.NoDedupe : InjectedBugs.None },
        };
        var withBug = new Simulation(1, Options(bug: true)).Run();
        Assert.False(withBug.Passed);
        Assert.Equal("Linearizability", withBug.FailureKind);
        var correct = new Simulation(1, Options(bug: false)).Run();
        Assert.True(correct.Passed, correct.Failure);
    }

    [Fact]
    public void A_partition_really_cuts_the_node_off_in_both_directions()
    {
        // With this seed node 3 wins the first election at about 174 ms. Cutting it off at 300 ms for 4 s:
        // if messages leaked in either direction, it would keep its followers (or hear the new leader) and
        // stay quiet. Cut off for real, it steps down by check-quorum and then times out over and over as
        // a candidate nobody can hear, while the other two elect a leader of their own.
        var options = new SimOptions { Nodes = 3, Clients = 4, Keys = 3 };
        var faults = new[] { new Fault(0, FaultKind.Partition, 300_000, 4_000_000, 0, new[] { 3 }, 0) };
        var trace = new StringWriter();
        var r = new Simulation(9, options, faults, trace).Run();
        Assert.True(r.Passed, r.Failure);
        Assert.Equal(1, r.FaultsFired);
        var lines = trace.ToString().Split('\n');
        Assert.Contains(lines, l => l.Contains("node 3 is Leader in term 1"));
        int candidacies = lines.Count(l => l.Contains("node 3 is Candidate"));
        Assert.True(candidacies >= 10, $"node 3 was a candidate only {candidacies} times");
        Assert.Contains(lines, l => l.Contains("node 1 is Leader") || l.Contains("node 2 is Leader"));

        // Control: without the partition node 3 stays leader and never campaigns again.
        var controlTrace = new StringWriter();
        var control = new Simulation(9, options, Array.Empty<Fault>(), controlTrace).Run();
        Assert.True(control.Passed, control.Failure);
        Assert.Equal(1, controlTrace.ToString().Split('\n').Count(l => l.Contains("node 3 is Candidate")));
        Assert.Equal(1, control.Elections);
    }
}
