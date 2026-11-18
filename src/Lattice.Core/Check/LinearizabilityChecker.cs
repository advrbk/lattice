using System.Text;

namespace Lattice.Check;

public enum CheckOutcome { Linearizable, NotLinearizable, GaveUp }

public sealed record CheckResult(CheckOutcome Outcome, string? Key, string Explanation, long StatesExplored)
{
    public bool Ok => Outcome == CheckOutcome.Linearizable;
}

/// <summary>
/// Decides whether a history of key-value operations is linearizable (Herlihy and Wing 1990).
///
/// The search is the Wing and Gong (1993) algorithm with Lowe's (2017) memoisation: walk the history in
/// time order, try to linearize any operation whose call has been seen, and backtrack when a return is
/// reached for an operation that has not been linearized yet. A cache of (set of linearized operations,
/// model state) pairs prunes configurations that were already explored, which is what makes it practical.
///
/// Every operation touches exactly one key, and a key-value map is the product of independent
/// registers, so the history is linearizable if and only if each per-key sub-history is (P-compositionality,
/// Horn and Kroening 2015). Each key is checked on its own, which keeps every search small.
/// </summary>
public static class LinearizabilityChecker
{
    public static CheckResult Check(IEnumerable<Operation> history, long maxStatesPerKey = 500_000)
    {
        var ops = history.ToList();
        for (int i = 0; i < ops.Count; i++) ops[i].Id = i;

        long explored = 0;
        foreach (var group in ops.GroupBy(o => o.Key, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var result = CheckKey(group.Key, group.ToList(), maxStatesPerKey);
            explored += result.StatesExplored;
            if (!result.Ok) return result with { StatesExplored = explored };
        }
        return new CheckResult(CheckOutcome.Linearizable, null, $"{ops.Count} operations linearizable", explored);
    }

    // ------------------------------------------------------------------ the sequential model

    /// <summary>
    /// The specification of one key: a register holding a string or nothing. Returns whether the
    /// operation's observed output is allowed from <paramref name="state"/>, and the state afterwards.
    /// Operations with unknown outcome accept any output.
    /// </summary>
    internal static bool Step(string? state, Operation op, out string? next)
    {
        switch (op.Kind)
        {
            case OpKind.Get:
                next = state;
                return !op.Completed || string.Equals(op.Output, state, StringComparison.Ordinal);
            case OpKind.Put:
                next = op.Value;
                return true;
            case OpKind.Delete:
                next = null;
                return true;
            case OpKind.Cas:
                bool matches = string.Equals(state, op.Expected, StringComparison.Ordinal);
                next = matches ? op.Value : state;
                if (!op.Completed) return true;
                // A failed cas also reports the value it found, which must be the current one.
                return op.CasSucceeded ? matches : !matches && string.Equals(op.Output, state, StringComparison.Ordinal);
            default:
                throw new InvalidOperationException();
        }
    }

    // ------------------------------------------------------------------ search over one key

    private sealed class Entry
    {
        public int Op;
        public bool IsCall;
        public Entry? Match;   // call -> its return
        public Entry? Prev;
        public Entry? Next;
    }

    private static CheckResult CheckKey(string key, List<Operation> all, long maxStates)
    {
        var ops = Prune(all);
        int n = ops.Count;
        if (n == 0) return new CheckResult(CheckOutcome.Linearizable, key, "empty", 0);

        // Build the event list: calls and returns sorted by time. On equal times calls go first, which
        // treats the two operations as concurrent (the more permissive reading).
        var events = new List<(long Time, int Kind, int Op)>(2 * n);
        for (int i = 0; i < n; i++)
        {
            events.Add((ops[i].Invoke, 0, i));
            events.Add((ops[i].Return, 1, i));
        }
        events.Sort((a, b) => a.Time != b.Time ? a.Time.CompareTo(b.Time) : a.Kind != b.Kind ? a.Kind.CompareTo(b.Kind) : a.Op.CompareTo(b.Op));

        var head = new Entry { Op = -1 };
        var calls = new Entry[n];
        var tail = head;
        foreach (var (_, kind, op) in events)
        {
            var e = new Entry { Op = op, IsCall = kind == 0, Prev = tail };
            tail.Next = e;
            tail = e;
            if (kind == 0) calls[op] = e;
            else calls[op].Match = e;
        }

        var linearized = new Bits(n);
        var cache = new HashSet<CacheKey>();
        var stack = new Stack<(Entry Call, string? StateBefore)>();
        string? state = null;
        Entry? entry = head.Next;
        long steps = 0;
        int deepest = 0;
        Bits? deepestSet = null;

        while (head.Next is not null)
        {
            if (++steps > maxStates)
                return new CheckResult(CheckOutcome.GaveUp, key, $"gave up on key {key} after {maxStates} steps ({n} operations)", steps);

            if (entry!.IsCall)
            {
                var op = ops[entry.Op];
                if (Step(state, op, out var next))
                {
                    linearized.Set(entry.Op);
                    if (cache.Add(new CacheKey(linearized.Clone(), next)))
                    {
                        stack.Push((entry, state));
                        state = next;
                        Lift(entry);
                        if (stack.Count > deepest) { deepest = stack.Count; deepestSet = linearized.Clone(); }
                        entry = head.Next;
                        continue;
                    }
                    linearized.Clear(entry.Op);
                }
                entry = entry.Next;
            }
            else
            {
                // We reached the return of an operation that has not been linearized: undo the last choice.
                if (stack.Count == 0)
                    return new CheckResult(CheckOutcome.NotLinearizable, key, Explain(key, ops, deepestSet), steps);
                var (call, before) = stack.Pop();
                state = before;
                linearized.Clear(call.Op);
                Unlift(call);
                entry = call.Next;
            }
        }
        return new CheckResult(CheckOutcome.Linearizable, key, $"{n} operations on {key} linearizable", steps);
    }

    /// <summary>
    /// Removes operations that cannot affect the verdict, which keeps the search small:
    /// <list type="bullet">
    /// <item>reads whose answer never arrived constrain nothing;</item>
    /// <item>a put or cas with unknown outcome whose new value nobody ever observed (no read returned it,
    /// no cas expected or found it) can always be placed at the very end of a linearization, where it
    /// changes nothing. Without this, every such operation stays "pending" until the end of the history
    /// and doubles the number of configurations the search can visit.</item>
    /// </list>
    /// </summary>
    internal static List<Operation> Prune(List<Operation> all)
    {
        var observed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var o in all)
        {
            if (o.Kind == OpKind.Get && o.Completed && o.Output is not null) observed.Add(o.Output);
            if (o.Kind == OpKind.Cas)
            {
                if (o.Expected is not null) observed.Add(o.Expected);
                if (o.Completed && !o.CasSucceeded && o.Output is not null) observed.Add(o.Output);
            }
        }
        return all.Where(o =>
        {
            if (o.Completed) return true;
            return o.Kind switch
            {
                OpKind.Get => false,
                OpKind.Put or OpKind.Cas => o.Value is null || observed.Contains(o.Value),
                _ => true,
            };
        }).ToList();
    }

    private static void Lift(Entry call)
    {
        call.Prev!.Next = call.Next;
        call.Next!.Prev = call.Prev;
        var ret = call.Match!;
        ret.Prev!.Next = ret.Next;
        if (ret.Next is not null) ret.Next.Prev = ret.Prev;
    }

    private static void Unlift(Entry call)
    {
        var ret = call.Match!;
        ret.Prev!.Next = ret;
        if (ret.Next is not null) ret.Next.Prev = ret;
        call.Prev!.Next = call;
        call.Next!.Prev = call;
    }

    private static string Explain(string key, List<Operation> ops, Bits? deepest)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"key {key}: no valid linearization of {ops.Count} operations.");
        var ordered = ops.Select((op, i) => (op, i)).OrderBy(x => x.op.Invoke).ToList();
        int frontier = deepest is null ? 0 : Math.Max(0, ordered.FindIndex(x => !deepest.Get(x.i)));
        if (deepest is not null)
            sb.AppendLine($"The longest linearizable prefix found covers {deepest.Count} operations. Operations around the " +
                          "first one that could not be placed are shown in invoke order; * marks those outside that prefix.");
        int from = Math.Max(0, frontier - 25), to = Math.Min(ordered.Count, frontier + 15);
        if (from > 0) sb.AppendLine($"  ... {from} earlier operations");
        for (int k = from; k < to; k++)
        {
            var (op, i) = ordered[k];
            bool inPrefix = deepest is not null && deepest.Get(i);
            sb.Append(inPrefix ? "    " : "  * ").AppendLine(op.ToString());
        }
        if (to < ordered.Count) sb.AppendLine($"  ... {ordered.Count - to} later operations");
        return sb.ToString();
    }

    // ------------------------------------------------------------------ small helpers

    private sealed class Bits
    {
        private readonly ulong[] _words;

        public Bits(int n) => _words = new ulong[(n + 63) / 64];
        private Bits(ulong[] words) => _words = words;

        public void Set(int i) => _words[i >> 6] |= 1UL << (i & 63);
        public void Clear(int i) => _words[i >> 6] &= ~(1UL << (i & 63));
        public bool Get(int i) => (_words[i >> 6] & (1UL << (i & 63))) != 0;
        public Bits Clone() => new((ulong[])_words.Clone());
        public int Count => _words.Sum(w => System.Numerics.BitOperations.PopCount(w));

        public bool SameAs(Bits other) => _words.AsSpan().SequenceEqual(other._words);

        public int Hash()
        {
            ulong h = 1469598103934665603UL;
            foreach (var w in _words) h = (h ^ w) * 1099511628211UL;
            return (int)(h ^ (h >> 32));
        }
    }

    private readonly struct CacheKey : IEquatable<CacheKey>
    {
        private readonly Bits _bits;
        private readonly string? _state;
        private readonly int _hash;

        public CacheKey(Bits bits, string? state)
        {
            _bits = bits;
            _state = state;
            _hash = HashCode.Combine(bits.Hash(), state is null ? 0 : StringComparer.Ordinal.GetHashCode(state));
        }

        public bool Equals(CacheKey other) =>
            _hash == other._hash && string.Equals(_state, other._state, StringComparison.Ordinal) && _bits.SameAs(other._bits);

        public override bool Equals(object? obj) => obj is CacheKey k && Equals(k);
        public override int GetHashCode() => _hash;
    }
}
