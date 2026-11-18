using Lattice.Check;
using Lattice.Util;
using static Lattice.Check.Operation;

namespace Lattice.Tests;

/// <summary>
/// More checker tests written during review: hand-made histories with cas, deletes and unknown
/// outcomes, a case that needs real backtracking, and a brute-force reference that shares no code with
/// the checker (its own model of a key-value map, applied to whole multi-key histories, so it also
/// tests the per-key split and the pruning of unobserved unknown writes).
/// </summary>
public class CheckerReviewTests
{
    private static CheckOutcome Check(params Operation[] ops) => LinearizabilityChecker.Check(ops).Outcome;

    [Fact]
    public void Search_backtracks_when_invoke_order_is_the_wrong_order()
    {
        // put(1) is invoked first, but the read after both puts returned 1, so put(2) must come first.
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "1", 1, 10),
            Put(1, "x", "2", 2, 9),
            Get(2, "x", "1", 11, 12)));
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "1", 1, 10),
            Put(1, "x", "2", 2, 9),
            Get(2, "x", "2", 11, 12)));
        // A third value was never written by anyone.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, 10),
            Put(1, "x", "2", 2, 9),
            Get(2, "x", "3", 11, 12)));
    }

    [Fact]
    public void Real_time_order_between_writes_is_respected()
    {
        // put(1) finished before put(2) started, so a later read cannot see 1 after put(2) finished.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, 2),
            Put(1, "x", "2", 3, 4),
            Get(2, "x", "1", 5, 6)));
    }

    [Fact]
    public void Cas_chain_with_a_concurrent_read_is_linearizable()
    {
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "1", true, 3, 6),
            Get(2, "x", "1", 4, 8),
            Cas(1, "x", "1", "2", true, 7, 10),
            Get(2, "x", "2", 11, 12)));
    }

    [Fact]
    public void Cas_on_an_absent_key()
    {
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Cas(0, "x", null, "a", true, 1, 2),
            Cas(1, "x", null, "b", false, 3, 4, found: "a")));
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Cas(0, "x", null, "a", true, 1, 2),
            Cas(1, "x", null, "b", true, 3, 4)));
        // A failed cas on an empty key must report "absent".
        Assert.Equal(CheckOutcome.Linearizable, Check(Cas(0, "x", "a", "b", false, 1, 2, found: null)));
        Assert.Equal(CheckOutcome.NotLinearizable, Check(Cas(0, "x", "a", "b", false, 1, 2, found: "a")));
    }

    [Fact]
    public void Unknown_cas_takes_effect_only_where_its_expected_value_holds()
    {
        // The unknown cas(a -> b) can only succeed before put(c); after that the read must see c.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "a", 1, 2),
            Cas(1, "x", "a", "b", true, 3, Unknown),
            Put(2, "x", "c", 4, 5),
            Get(3, "x", "b", 6, 7)));
        // Seeing b while put(c) is still running is fine.
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "a", 1, 2),
            Cas(1, "x", "a", "b", true, 3, Unknown),
            Get(3, "x", "b", 4, 5),
            Put(2, "x", "c", 6, 7),
            Get(3, "x", "c", 8, 9)));
    }

    [Fact]
    public void Unknown_cas_that_was_observed_forces_a_later_cas_to_fail()
    {
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "1", true, 3, Unknown),
            Get(2, "x", "1", 4, 5),
            Cas(3, "x", "0", "2", true, 6, 7)));
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "1", true, 3, Unknown),
            Get(2, "x", "1", 4, 5),
            Cas(3, "x", "0", "2", false, 6, 7, found: "1")));
    }

    [Fact]
    public void Unknown_delete_cannot_be_undone()
    {
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "1", 1, 2),
            Delete(1, "x", 3, Unknown),
            Get(2, "x", null, 4, 5),
            Get(2, "x", "1", 6, 7)));
        Assert.Equal(CheckOutcome.Linearizable, Check(
            Put(0, "x", "1", 1, 2),
            Delete(1, "x", 3, Unknown),
            Get(2, "x", "1", 4, 5),
            Get(2, "x", null, 6, 7)));
    }

    [Fact]
    public void Lost_acknowledged_write_is_caught()
    {
        // The shape of a Raft bug that loses a committed entry: an acknowledged write disappears.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "a", 1, 2),
            Put(1, "x", "b", 3, 4),
            Get(2, "x", "b", 5, 6),
            Get(2, "x", "a", 7, 8)));
    }

    [Fact]
    public void Retried_cas_applied_twice_is_caught()
    {
        // The no-dedupe bug: the client's own cas applied, the reply was lost, the retry failed and
        // reported the client's own new value.
        Assert.Equal(CheckOutcome.NotLinearizable, Check(
            Put(0, "x", "0", 1, 2),
            Cas(1, "x", "0", "1.7", false, 3, 6, found: "1.7")));
    }

    [Fact]
    public void Checker_agrees_with_an_independent_brute_force_on_multi_key_histories()
    {
        var rng = new DetRandom(77);
        int bad = 0, unknownOps = 0;
        for (int round = 0; round < 3000; round++)
        {
            var ops = RandomHistory(rng, rng.Next(2, 8));
            unknownOps += ops.Count(o => !o.Completed);
            bool expected = Reference.IsLinearizable(ops);
            var actual = LinearizabilityChecker.Check(ops).Outcome;
            Assert.True((expected ? CheckOutcome.Linearizable : CheckOutcome.NotLinearizable) == actual,
                $"round {round}: reference says {expected}, checker says {actual}\n{string.Join("\n", ops)}");
            if (!expected) bad++;
        }
        Assert.InRange(bad, 300, 2700);
        Assert.True(unknownOps > 1000);
    }

    [Fact]
    public void Checker_is_not_fooled_by_histories_that_are_almost_linearizable()
    {
        // Start from a linearizable history built by a sequential run, then corrupt one read.
        var rng = new DetRandom(5);
        int caught = 0;
        for (int round = 0; round < 500; round++)
        {
            var ops = SequentialThenOverlap(rng, 8);
            Assert.Equal(CheckOutcome.Linearizable, LinearizabilityChecker.Check(ops).Outcome);
            var reads = ops.Where(o => o.Kind == OpKind.Get && o.Completed).ToList();
            if (reads.Count == 0) continue;
            var victim = reads[rng.Next(reads.Count)];
            victim.Output = "never-written";
            Assert.Equal(CheckOutcome.NotLinearizable, LinearizabilityChecker.Check(ops).Outcome);
            caught++;
        }
        Assert.True(caught > 300);
    }

    // ------------------------------------------------------------------ generators

    private static List<Operation> RandomHistory(DetRandom rng, int n)
    {
        string?[] values = { null, "a", "b" };
        string[] keys = { "x", "y" };
        var ops = new List<Operation>();
        for (int i = 0; i < n; i++)
        {
            string key = keys[rng.Next(keys.Length)];
            long invoke = rng.Next(0, 16);
            long ret = rng.Chance(0.2) ? Unknown : invoke + 1 + rng.Next(0, 6);
            string? v = values[rng.Next(3)];
            ops.Add(rng.Next(4) switch
            {
                0 => Get(i, key, v, invoke, ret),
                1 => Put(i, key, v ?? "c", invoke, ret),
                2 => Delete(i, key, invoke, ret),
                _ => Cas(i, key, v, rng.Chance(0.2) ? null : values[rng.Next(3)] ?? "c", rng.Chance(0.5), invoke, ret, found: values[rng.Next(3)]),
            });
        }
        return ops;
    }

    /// <summary>
    /// Runs random operations one after another against a real dictionary (so the result is
    /// linearizable), then widens each operation's interval so operations overlap.
    /// </summary>
    private static List<Operation> SequentialThenOverlap(DetRandom rng, int n)
    {
        var state = new Dictionary<string, string?>();
        var ops = new List<Operation>();
        for (int i = 0; i < n; i++)
        {
            string key = rng.Chance(0.5) ? "x" : "y";
            state.TryGetValue(key, out var cur);
            long at = 10 * i + 5; // the instant the operation takes effect
            long invoke = at - rng.Next(0, 15), ret = at + rng.Next(1, 15);
            switch (rng.Next(4))
            {
                case 0:
                    ops.Add(Get(i, key, cur, invoke, ret));
                    break;
                case 1:
                    state[key] = $"v{i}";
                    ops.Add(Put(i, key, $"v{i}", invoke, ret));
                    break;
                case 2:
                    state[key] = null;
                    ops.Add(Delete(i, key, invoke, ret));
                    break;
                default:
                    string? expected = rng.Chance(0.6) ? cur : "other";
                    bool ok = expected == cur;
                    if (ok) state[key] = $"v{i}";
                    ops.Add(Cas(i, key, expected, $"v{i}", ok, invoke, ret, found: ok ? null : cur));
                    break;
            }
        }
        return ops;
    }

    /// <summary>
    /// A deliberately naive reference: tries every order of the operations that respects real time,
    /// on the whole multi-key history at once, with its own model of the map. An operation with an
    /// unknown outcome may be applied (with its real effect) or skipped.
    /// </summary>
    private static class Reference
    {
        public static bool IsLinearizable(List<Operation> ops)
        {
            var done = new bool[ops.Count];
            return Search(new Dictionary<string, string>(), 0);

            bool Search(Dictionary<string, string> map, int count)
            {
                if (count == ops.Count) return true;
                for (int i = 0; i < ops.Count; i++)
                {
                    if (done[i] || !Minimal(i)) continue;
                    done[i] = true;
                    var after = new Dictionary<string, string>(map);
                    if (Apply(after, ops[i]) && Search(after, count + 1)) return true;
                    if (ops[i].Return == Unknown && Search(map, count + 1)) return true;
                    done[i] = false;
                }
                return false;
            }

            // i can be next only if every operation that returned before i was invoked is already placed.
            bool Minimal(int i)
            {
                for (int j = 0; j < ops.Count; j++)
                    if (!done[j] && j != i && ops[j].Return < ops[i].Invoke) return false;
                return true;
            }
        }

        private static bool Apply(Dictionary<string, string> map, Operation op)
        {
            bool known = op.Return != Unknown;
            bool present = map.TryGetValue(op.Key, out var current);
            switch (op.Kind)
            {
                case OpKind.Get:
                    return !known || (present ? op.Output == current : op.Output is null);
                case OpKind.Put:
                    map[op.Key] = op.Value!;
                    return true;
                case OpKind.Delete:
                    map.Remove(op.Key);
                    return true;
                case OpKind.Cas:
                    bool holds = op.Expected is null ? !present : present && current == op.Expected;
                    if (holds)
                    {
                        if (op.Value is null) map.Remove(op.Key);
                        else map[op.Key] = op.Value;
                    }
                    if (!known) return true;
                    if (op.CasSucceeded) return holds;
                    return !holds && (present ? op.Output == current : op.Output is null);
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
