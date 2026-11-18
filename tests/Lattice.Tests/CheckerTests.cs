using Lattice.Check;
using Lattice.Util;
using static Lattice.Check.Operation;

namespace Lattice.Tests;

public class CheckerTests
{
    private static CheckOutcome Check(params Operation[] ops) => LinearizabilityChecker.Check(ops).Outcome;

    [Fact]
    public void Sequential_history_is_linearizable()
    {
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "1", 1, 2),
            Get(0, "x", "1", 3, 4),
            Cas(0, "x", "1", "2", true, 5, 6),
            Get(1, "x", "2", 7, 8),
            Delete(1, "x", 9, 10),
            Get(0, "x", null, 11, 12)));
    }

    [Fact]
    public void Stale_read_after_a_completed_write_is_not_linearizable()
    {
        // put(x,1) finished before the read started, yet the read returned the old (absent) value.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, 2),
            Get(1, "x", null, 3, 4)));
    }

    [Fact]
    public void Concurrent_read_may_see_either_value()
    {
        Assert.Equal(CheckOutcome.Linearizable, Check(Put(0, "x", "1", 1, 10), Get(1, "x", null, 2, 3)));
        Assert.Equal(CheckOutcome.Linearizable, Check(Put(0, "x", "1", 1, 10), Get(1, "x", "1", 2, 3)));
    }

    [Fact]
    public void Reads_cannot_go_back_in_time()
    {
        // Two sequential reads during a long write: once one read sees the new value, a later read cannot see the old one.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, 100),
            Get(1, "x", "1", 2, 3),
            Get(2, "x", null, 4, 5)));
    }

    [Fact]
    public void Two_successful_cas_from_the_same_value_are_not_linearizable()
    {
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "a", true, 3, 10),
            Cas(2, "x", "0", "b", true, 4, 11)));
    }

    [Fact]
    public void Failed_cas_must_be_explained_by_some_other_value()
    {
        // x was "0" and nobody else wrote, so a failing cas from "0" is impossible.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "a", false, 3, 4)));
        // With a concurrent write it is fine.
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "0", 1, 2),
            Put(2, "x", "z", 3, 6),
            Cas(1, "x", "0", "a", false, 4, 5, found: "z")));
        // But it must report the value it actually found.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "0", 1, 2),
            Put(2, "x", "z", 3, 6),
            Cas(1, "x", "0", "a", false, 4, 5, found: "q")));
    }

    [Fact]
    public void Unknown_write_may_or_may_not_have_happened()
    {
        var applied = new[] { Put(0, "x", "1", 1, Unknown), Get(1, "x", "1", 5, 6) };
        var notApplied = new[] { Put(0, "x", "1", 1, Unknown), Get(1, "x", null, 5, 6) };
        Assert.Equal(CheckOutcome.Linearizable, LinearizabilityChecker.Check(applied).Outcome);
        Assert.Equal(CheckOutcome.Linearizable, LinearizabilityChecker.Check(notApplied).Outcome);
    }

    [Fact]
    public void Unknown_write_cannot_take_effect_before_it_was_invoked()
    {
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Get(1, "x", "1", 1, 2),
            Put(0, "x", "1", 3, Unknown)));
    }

    [Fact]
    public void Unknown_write_cannot_take_effect_twice()
    {
        // The read sequence 1, 2, 1 needs put(1) to happen twice.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, Unknown),
            Get(1, "x", "1", 2, 3),
            Put(1, "x", "2", 4, 5),
            Get(1, "x", "1", 6, 7)));
    }

    [Fact]
    public void Unobserved_unknown_writes_are_pruned_but_observed_ones_are_kept()
    {
        var ops = new List<Operation>
        {
            Put(0, "x", "seen", 1, Unknown),
            Put(1, "x", "never-read", 2, Unknown),
            Cas(2, "x", "a", "also-never-read", false, 3, Unknown),
            Get(3, "x", null, 4, Unknown),
            Get(4, "x", "seen", 5, 6),
        };
        var kept = LinearizabilityChecker.Prune(ops);
        Assert.Equal(new[] { "seen", null }, kept.Select(o => o.Kind == OpKind.Get ? null : o.Value));
    }

    [Fact]
    public void Keys_are_checked_independently()
    {
        var result = LinearizabilityChecker.Check(new[]
        {
            Put(0, "a", "1", 1, 2), Get(1, "a", "1", 3, 4),
            Put(0, "b", "1", 1, 2), Get(1, "b", null, 3, 4), // stale on b only
        });
        Assert.Equal(CheckOutcome.NotLinearizable, result.Outcome);
        Assert.Equal("b", result.Key);
        Assert.Contains("get(b)", result.Explanation);
    }

    [Fact]
    public void Checker_agrees_with_brute_force_on_random_small_histories()
    {
        var rng = new DetRandom(2024);
        int nonLinearizable = 0;
        for (int round = 0; round < 3000; round++)
        {
            var ops = RandomHistory(rng, rng.Next(2, 7));
            bool brute = BruteForce(ops);
            var fast = LinearizabilityChecker.Check(ops).Outcome;
            Assert.Equal(brute ? CheckOutcome.Linearizable : CheckOutcome.NotLinearizable, fast);
            if (!brute) nonLinearizable++;
        }
        Assert.InRange(nonLinearizable, 300, 2700); // the generator produces a real mix of both outcomes
    }

    /// <summary>Random overlapping operations on one key with random (often wrong) outputs.</summary>
    private static List<Operation> RandomHistory(DetRandom rng, int n)
    {
        string?[] values = { null, "a", "b" };
        var ops = new List<Operation>();
        for (int i = 0; i < n; i++)
        {
            long invoke = rng.Next(0, 20);
            long ret = rng.Chance(0.15) ? Unknown : invoke + 1 + rng.Next(0, 8);
            string? v = values[rng.Next(3)];
            ops.Add(rng.Next(4) switch
            {
                0 => Get(i, "x", v, invoke, ret),
                1 => Put(i, "x", v ?? "c", invoke, ret),
                2 => Delete(i, "x", invoke, ret),
                _ => Cas(i, "x", v, values[rng.Next(3)] ?? "c", rng.Chance(0.5), invoke, ret, found: values[rng.Next(3)]),
            });
        }
        return ops;
    }

    /// <summary>Tries every order that respects real time: a must precede b if a returned before b was invoked.</summary>
    private static bool BruteForce(List<Operation> ops)
    {
        // Reads that never answered constrain nothing; an unknown write may be left out entirely.
        var live = ops.Where(o => o.Completed || o.Kind != OpKind.Get).ToList();
        var used = new bool[live.Count];
        return Search(null, 0);

        bool Search(string? state, int placed)
        {
            if (placed == live.Count) return true;
            for (int i = 0; i < live.Count; i++)
            {
                if (used[i]) continue;
                // i may go next only if no unplaced operation finished before i started.
                bool blocked = false;
                for (int j = 0; j < live.Count && !blocked; j++)
                    if (!used[j] && j != i && live[j].Return < live[i].Invoke) blocked = true;
                if (blocked) continue;
                used[i] = true;
                if (LinearizabilityChecker.Step(state, live[i], out var next) && Search(next, placed + 1)) return true;
                // An unknown operation may also never take effect.
                if (!live[i].Completed && Search(state, placed + 1)) return true;
                used[i] = false;
            }
            return false;
        }
    }
}
