using Lattice.Net;

namespace Lattice.Cli;

internal static class ClientCommands
{
    private const string DefaultNodes = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003";

    public static async Task<int> Run(string command, Args a)
    {
        var nodes = LatticeClient.ParseEndpoints(a.Get("nodes", DefaultNodes));
        int timeoutSeconds = a.Int("timeout", 10);
        bool absent = a.Flag("absent");
        a.RejectUnknown();
        var p = a.Positional;

        if (command == "status") return await Status(nodes);

        await using var client = new LatticeClient(nodes) { Timeout = TimeSpan.FromSeconds(timeoutSeconds) };
        try
        {
            switch (command)
            {
                case "put":
                    Need(p, 2, "put <key> <value>");
                    var put = await client.PutAsync(p[0], p[1]);
                    Console.WriteLine($"OK (leader {put.Leader})");
                    return 0;
                case "get":
                    Need(p, 1, "get <key>");
                    var get = await client.GetAsync(p[0]);
                    Console.WriteLine(get.Value ?? "(not found)");
                    return get.Value is null ? 1 : 0;
                case "delete":
                    Need(p, 1, "delete <key>");
                    var del = await client.DeleteAsync(p[0]);
                    Console.WriteLine(del.Success ? "deleted" : "(not found)");
                    return 0;
                case "cas":
                    string? expected;
                    string value;
                    if (absent)
                    {
                        Need(p, 2, "cas <key> --absent <new>");
                        expected = null;
                        value = p[1];
                    }
                    else
                    {
                        Need(p, 3, "cas <key> <expected> <new>");
                        expected = p[1];
                        value = p[2];
                    }
                    var cas = await client.CasAsync(p[0], expected, value);
                    Console.WriteLine(cas.Success ? "swapped" : $"not swapped, current value: {cas.Value ?? "(not found)"}");
                    return cas.Success ? 0 : 1;
            }
        }
        catch (OutcomeUnknownException ex)
        {
            Console.Error.WriteLine($"error: {ex.Message}");
            return 3;
        }
        return 2;
    }

    private static void Need(List<string> p, int n, string usage)
    {
        if (p.Count != n) throw new ArgumentException($"usage: lattice {usage}");
    }

    private static async Task<int> Status(List<System.Net.IPEndPoint> nodes)
    {
        var all = await Task.WhenAll(nodes.Select(n => LatticeClient.StatusAsync(n, TimeSpan.FromSeconds(2))));
        Console.WriteLine($"{"node",-6}{"address",-18}{"role",-11}{"term",6}{"leader",8}{"commit",9}{"applied",9}{"last log",10}{"snapshot",10}{"keys",7}");
        for (int i = 0; i < nodes.Count; i++)
        {
            var s = all[i];
            if (s is null)
                Console.WriteLine($"{i + 1,-6}{nodes[i],-18}{"down",-11}");
            else
                Console.WriteLine($"{s.Id,-6}{nodes[i],-18}{s.Role,-11}{s.Term,6}{(s.LeaderId == 0 ? "-" : s.LeaderId.ToString()),8}{s.CommitIndex,9}{s.LastApplied,9}{s.LastLogIndex,10}{s.SnapshotIndex,10}{s.Keys,7}");
        }
        return all.Any(s => s is not null) ? 0 : 1;
    }
}
