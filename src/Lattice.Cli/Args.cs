namespace Lattice.Cli;

/// <summary>Tiny argument parser: positional words plus --name value / --flag options.</summary>
internal sealed class Args
{
    private readonly Dictionary<string, string?> _options = new(StringComparer.Ordinal);
    private readonly HashSet<string> _used = new(StringComparer.Ordinal);

    public Args(IEnumerable<string> args, params string[] flags)
    {
        var list = args.ToList();
        for (int i = 0; i < list.Count; i++)
        {
            string a = list[i];
            if (a.StartsWith("--", StringComparison.Ordinal))
            {
                string name = a[2..];
                if (flags.Contains(name) || i + 1 >= list.Count) _options[name] = null;
                else _options[name] = list[++i];
            }
            else
            {
                Positional.Add(a);
            }
        }
    }

    public List<string> Positional { get; } = new();

    public bool Flag(string name)
    {
        _used.Add(name);
        return _options.ContainsKey(name);
    }

    public string? Get(string name)
    {
        _used.Add(name);
        return _options.TryGetValue(name, out var v) ? v : null;
    }

    public string Get(string name, string fallback) => Get(name) ?? fallback;

    public int Int(string name, int fallback) => Get(name) is { } s ? int.Parse(s) : fallback;

    public ulong ULong(string name, ulong fallback) => Get(name) is { } s ? ulong.Parse(s) : fallback;

    public void RejectUnknown()
    {
        var unknown = _options.Keys.Where(k => !_used.Contains(k)).ToList();
        if (unknown.Count > 0) throw new ArgumentException($"unknown option(s): {string.Join(", ", unknown.Select(u => "--" + u))}");
    }
}
