using Lattice.Cli;

const string Usage = """
Lattice: a replicated key-value store on Raft.

Cluster:
  lattice serve --id 1 --cluster 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 --data data/n1 [--read-mode readindex|log]
  lattice cluster [--nodes 3] [--port 7001] [--data data]      start a local cluster in the foreground

Client (default --nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003, listed in node id order):
  lattice put <key> <value>
  lattice get <key>
  lattice delete <key>
  lattice cas <key> <expected> <new>        use --absent instead of <expected> to require a missing key
  lattice status

Testing:
  lattice chaos --seeds 10000 [--start 1] [--nodes 3|5] [--bug name] [--parallel 2] [--no-invariants] [--stop]
  lattice chaos --seed 42 [--trace] [--shrink] [--faults 0,3|none]     replay one seed
  lattice bugs [--seeds 2000] [--only name]  how fast each injected bug is caught
  lattice bench [--seconds 10] [--clients 32] [--failovers 15] [--sim-seeds 200]

Bugs: vote-no-log-check, commit-old-term, skip-fsync, stale-read, no-dedupe
""";

if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    Console.WriteLine(Usage);
    return 0;
}

try
{
    var rest = args.Skip(1);
    return args[0] switch
    {
        "chaos" => ChaosCommands.Chaos(new Args(rest, "trace", "shrink", "no-invariants", "stop")),
        "bugs" => ChaosCommands.Bugs(new Args(rest)),
        "serve" => ClusterCommands.Serve(new Args(rest)),
        "cluster" => ClusterCommands.Cluster(new Args(rest)),
        "put" or "get" or "delete" or "cas" or "status" => await ClientCommands.Run(args[0], new Args(rest, "absent")),
        "bench" => await BenchCommand.Run(new Args(rest)),
        _ => throw new ArgumentException($"unknown command '{args[0]}'"),
    };
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    Console.Error.WriteLine("run 'lattice --help' for usage");
    return 2;
}
