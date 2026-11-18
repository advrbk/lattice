using System.Diagnostics;
using Lattice.Kv;
using Lattice.Net;

namespace Lattice.Cli;

internal static class ClusterCommands
{
    /// <summary>lattice serve --id N --cluster a:p,b:p,c:p --data dir [--read-mode readindex|log] [--parent-pid P]</summary>
    public static int Serve(Args a)
    {
        int id = int.Parse(a.Get("id") ?? throw new ArgumentException("--id is required"));
        var cluster = LatticeClient.ParseEndpoints(a.Get("cluster") ?? throw new ArgumentException("--cluster is required"));
        string data = a.Get("data", Path.Combine("data", $"n{id}"));
        var readMode = ParseReadMode(a.Get("read-mode", "readindex"));
        int parentPid = a.Int("parent-pid", 0);
        a.RejectUnknown();
        if (id < 1 || id > cluster.Count) throw new ArgumentException($"--id must be between 1 and {cluster.Count}");

        var host = new ServerHost(new ServerConfig
        {
            Id = id,
            Cluster = cluster,
            DataDirectory = data,
            ReadMode = readMode,
            Log = Console.Out,
        });
        host.Start();

        using var stop = new ManualResetEventSlim();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stop.Set();
        };
        if (parentPid > 0)
        {
            // Exit with the process that started us, so no node is ever left running on its own.
            new Thread(() =>
            {
                while (!stop.IsSet)
                {
                    try
                    {
                        using var parent = Process.GetProcessById(parentPid);
                        if (parent.HasExited) break;
                    }
                    catch (ArgumentException) { break; }
                    Thread.Sleep(500);
                }
                stop.Set();
            }) { IsBackground = true }.Start();
        }
        stop.Wait();
        host.Dispose();
        return 0;
    }

    /// <summary>lattice cluster [--nodes 3] [--port 7001] [--data data]: runs a local cluster until Enter or Ctrl+C.</summary>
    public static int Cluster(Args a)
    {
        int nodes = a.Int("nodes", 3);
        int port = a.Int("port", 7001);
        string data = a.Get("data", "data");
        string readMode = a.Get("read-mode", "readindex");
        ParseReadMode(readMode);
        a.RejectUnknown();

        using var cluster = new LocalCluster(nodes, port, data, echo: true, readMode);
        cluster.StartAll();
        for (int id = 1; id <= nodes; id++)
            Console.WriteLine($"node {id}: pid {cluster.Pid(id)}, {cluster.Endpoints[id - 1]}, data {Path.Combine(data, $"n{id}")}");
        Console.WriteLine("Press Enter or Ctrl+C to stop all nodes.");

        using var stop = new ManualResetEventSlim();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stop.Set();
        };
        new Thread(() =>
        {
            string? line = null;
            try { line = Console.ReadLine(); } catch (IOException) { }
            if (line is not null) stop.Set(); // no console input at all: wait for Ctrl+C instead

        }) { IsBackground = true }.Start();
        stop.Wait();
        Console.WriteLine("Stopping nodes...");
        return 0; // disposing the cluster kills every node by PID
    }

    public static ReadMode ParseReadMode(string s) => s.ToLowerInvariant() switch
    {
        "readindex" => ReadMode.ReadIndex,
        "log" => ReadMode.Log,
        _ => throw new ArgumentException("--read-mode must be readindex or log"),
    };
}
