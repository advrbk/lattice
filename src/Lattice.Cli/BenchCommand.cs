using System.Diagnostics;
using Lattice.Check;
using Lattice.Net;
using Lattice.Raft;
using Lattice.Sim;

namespace Lattice.Cli;

/// <summary>
/// lattice bench [--nodes 3] [--seconds 10] [--clients 32] [--failovers 15] [--sim-seeds 200] [--port 7101]
/// Starts a real local cluster as separate processes, measures write and read throughput and latency,
/// checks a mixed workload for linearizability, kills the leader repeatedly to time failover, then
/// measures simulator speed. Every node it starts is killed by PID before it returns.
/// </summary>
internal static class BenchCommand
{
    public static async Task<int> Run(Args a)
    {
        int nodes = a.Int("nodes", 3);
        int seconds = a.Int("seconds", 10);
        int clients = a.Int("clients", 32);
        int failovers = a.Int("failovers", 15);
        int simSeeds = a.Int("sim-seeds", 200);
        int port = a.Int("port", 7101);
        string readMode = a.Get("read-mode", "readindex");
        bool skipCluster = a.Flag("sim-only");
        a.RejectUnknown();

        Console.WriteLine($"Machine: {Environment.ProcessorCount} logical CPUs, {Environment.OSVersion}, .NET {Environment.Version}");
        Console.WriteLine($"Date: {DateTime.Now:yyyy-MM-dd HH:mm}");
        Console.WriteLine();

        if (!skipCluster)
        {
            string data = Path.Combine("bench-data", DateTime.Now.ToString("yyyyMMdd-HHmmss"));
            using var cluster = new LocalCluster(nodes, port, data, echo: false, readMode);
            try
            {
                cluster.StartAll();
                Console.WriteLine($"Started {nodes} node processes (pids {string.Join(", ", Enumerable.Range(1, nodes).Select(i => cluster.Pid(i)))}), data in {data}, reads via {readMode}");
                var leader = await cluster.WaitForLeaderAsync(TimeSpan.FromSeconds(20))
                             ?? throw new InvalidOperationException("no leader elected within 20 s");
                Console.WriteLine($"Leader: node {leader.Id} (term {leader.Term})");

                await Load(cluster, clients, 2, write: true, quiet: true); // warm-up
                Console.WriteLine();
                Console.WriteLine($"Throughput and latency, {clients} concurrent clients, {seconds} s each, fsync on every batch:");
                await Load(cluster, clients, seconds, write: true, quiet: false);
                await Load(cluster, clients, seconds, write: false, quiet: false);
                await Load(cluster, 1, Math.Max(3, seconds / 2), write: true, quiet: false);

                Console.WriteLine();
                await CheckRealHistory(cluster, 8, 5);

                if (failovers > 0)
                {
                    Console.WriteLine();
                    await Failover(cluster, failovers);
                }
            }
            finally
            {
                cluster.Dispose();
                Console.WriteLine("All node processes killed.");
            }
        }

        if (simSeeds > 0)
        {
            Console.WriteLine();
            SimSpeed(simSeeds);
        }
        return 0;
    }

    private static async Task Load(LocalCluster cluster, int clients, int seconds, bool write, bool quiet)
    {
        var latencies = new List<double>[clients];
        long failures = 0;
        var stopAt = Stopwatch.GetTimestamp() + (long)(seconds * Stopwatch.Frequency);
        var tasks = Enumerable.Range(0, clients).Select(async c =>
        {
            var lat = latencies[c] = new List<double>(100_000);
            await using var client = new LatticeClient(cluster.Endpoints) { Timeout = TimeSpan.FromSeconds(5) };
            int i = 0;
            while (Stopwatch.GetTimestamp() < stopAt)
            {
                string key = $"key-{c}-{i % 1000}";
                long t0 = Stopwatch.GetTimestamp();
                try
                {
                    if (write) await client.PutAsync(key, $"value-{i}");
                    else await client.GetAsync(key);
                    lat.Add(Stopwatch.GetElapsedTime(t0).TotalMilliseconds);
                }
                catch (OutcomeUnknownException)
                {
                    Interlocked.Increment(ref failures);
                }
                i++;
            }
        }).ToArray();
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        sw.Stop();
        if (quiet) return;

        var all = latencies.SelectMany(l => l).OrderBy(x => x).ToArray();
        double rate = all.Length / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"  {(write ? "put" : "get"),-4} {clients,3} clients: {rate,9:N0} ops/s   p50 {Pct(all, 0.50),6:0.00} ms   p99 {Pct(all, 0.99),6:0.00} ms   max {all.LastOrDefault(),7:0.0} ms   ({all.Length:N0} ops, {failures} timed out)");
    }

    private static double Pct(double[] sorted, double p) =>
        sorted.Length == 0 ? 0 : sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(p * sorted.Length) - 1)];

    /// <summary>A mixed workload on a few keys against the real cluster, recorded and checked like a simulator run.</summary>
    private static async Task CheckRealHistory(LocalCluster cluster, int clients, int seconds)
    {
        var history = new List<Operation>();
        var gate = new object();
        var stopAt = Stopwatch.GetTimestamp() + (long)(seconds * Stopwatch.Frequency);
        var tasks = Enumerable.Range(0, clients).Select(async c =>
        {
            var rng = new Random(c);
            await using var client = new LatticeClient(cluster.Endpoints) { Timeout = TimeSpan.FromSeconds(3) };
            string? lastSeen = null;
            for (int i = 0; Stopwatch.GetTimestamp() < stopAt; i++)
            {
                string key = $"lin-{rng.Next(3)}";
                string unique = $"{c}.{i}";
                int roll = rng.Next(100);
                var op = roll < 40 ? new Operation { Client = c, Key = key, Kind = OpKind.Get }
                    : roll < 75 ? new Operation { Client = c, Key = key, Kind = OpKind.Put, Value = unique }
                    : roll < 85 ? new Operation { Client = c, Key = key, Kind = OpKind.Delete }
                    : new Operation { Client = c, Key = key, Kind = OpKind.Cas, Expected = lastSeen, Value = unique };
                op.Invoke = Stopwatch.GetTimestamp();
                try
                {
                    var r = op.Kind switch
                    {
                        OpKind.Get => await client.GetAsync(key),
                        OpKind.Put => await client.PutAsync(key, unique),
                        OpKind.Delete => await client.DeleteAsync(key),
                        _ => await client.CasAsync(key, lastSeen, unique),
                    };
                    op.Return = Stopwatch.GetTimestamp();
                    op.Output = r.Value;
                    op.CasSucceeded = r.Success;
                    lastSeen = op.Kind == OpKind.Get ? r.Value : op.Kind == OpKind.Cas && !r.Success ? r.Value : lastSeen;
                }
                catch (OutcomeUnknownException) { /* stays unknown */ }
                lock (gate) history.Add(op);
            }
        }).ToArray();
        await Task.WhenAll(tasks);
        var sw = Stopwatch.StartNew();
        var result = LinearizabilityChecker.Check(history);
        Console.WriteLine($"Real-cluster history: {history.Count:N0} mixed operations by {clients} clients on 3 keys -> {result.Outcome} " +
                          $"({result.StatesExplored:N0} search steps, {sw.ElapsedMilliseconds} ms)");
    }

    private static async Task Failover(LocalCluster cluster, int runs)
    {
        Console.WriteLine($"Failover: kill the leader process {runs} times (election timeout 150-300 ms, heartbeat 30 ms):");
        var elect = new List<double>();
        var write = new List<double>();
        for (int run = 1; run <= runs; run++)
        {
            var leader = await cluster.WaitForLeaderAsync(TimeSpan.FromSeconds(20))
                         ?? throw new InvalidOperationException("no leader before failover run");
            // Let the cluster settle so every follower has a fresh heartbeat timer.
            await Task.Delay(500);
            leader = await cluster.WaitForLeaderAsync(TimeSpan.FromSeconds(20)) ?? leader;

            var t0 = Stopwatch.GetTimestamp();
            cluster.Kill(leader.Id);
            var writer = Task.Run(async () =>
            {
                await using var client = new LatticeClient(cluster.Endpoints)
                {
                    Timeout = TimeSpan.FromSeconds(20), AttemptTimeout = TimeSpan.FromMilliseconds(100),
                };
                await client.PutAsync("failover", run.ToString());
                return Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            });
            var next = await cluster.WaitForLeaderAsync(TimeSpan.FromSeconds(20), excluding: leader.Id, minTerm: leader.Term);
            double electedMs = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            double writeMs = await writer;
            if (next is null) throw new InvalidOperationException("no new leader within 20 s");
            elect.Add(electedMs);
            write.Add(writeMs);
            Console.WriteLine($"  run {run,2}: killed node {leader.Id} (term {leader.Term}); node {next.Value.Id} leader of term {next.Value.Term} after {electedMs,6:0} ms; first write accepted after {writeMs,6:0} ms");

            cluster.Start(leader.Id);
            await Task.Delay(1000);
        }
        elect.Sort();
        write.Sort();
        Console.WriteLine($"  new leader elected: median {Median(elect):0} ms, min {elect[0]:0} ms, max {elect[^1]:0} ms");
        Console.WriteLine($"  first write accepted: median {Median(write):0} ms, min {write[0]:0} ms, max {write[^1]:0} ms");
    }

    private static double Median(List<double> sorted) =>
        sorted.Count % 2 == 1 ? sorted[sorted.Count / 2] : (sorted[sorted.Count / 2 - 1] + sorted[sorted.Count / 2]) / 2;

    private static void SimSpeed(int seeds)
    {
        Console.WriteLine($"Simulator speed: {seeds} chaos seeds on one thread (each ~8 s simulated, invariants and linearizability check included):");
        ChaosRunner.RunSeed(999_999, null, InjectedBugs.None); // JIT warm-up
        var sw = Stopwatch.StartNew();
        var s = ChaosRunner.Run(new ChaosOptions { FirstSeed = 1_000_000, Seeds = seeds, Parallelism = 1 });
        sw.Stop();
        Console.WriteLine($"  {s.SeedsRun / sw.Elapsed.TotalSeconds:0.0} seeds/s, {s.Events / sw.Elapsed.TotalSeconds / 1e6:0.00} M events/s, " +
                          $"{s.SimMicros / 1e6 / sw.Elapsed.TotalSeconds:0} simulated seconds per wall second, {s.Failures} failures");
    }
}
