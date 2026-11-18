using System.Diagnostics;
using System.Net;
using Lattice.Net;
using Lattice.Raft;

namespace Lattice.Cli;

/// <summary>
/// Starts cluster nodes as separate OS processes (this same executable with "serve") and keeps their
/// Process handles so every one of them can be killed by PID. Each child also gets --parent-pid and
/// exits on its own if this process disappears, so a crashed parent cannot leave nodes behind.
/// </summary>
internal sealed class LocalCluster : IDisposable
{
    private readonly Process?[] _procs;
    private readonly StreamWriter?[] _logs;
    private readonly string _dataRoot;
    private readonly bool _echo;
    private readonly string _readMode;

    public LocalCluster(int nodes, int basePort, string dataRoot, bool echo, string readMode = "readindex")
    {
        Endpoints = Enumerable.Range(0, nodes).Select(i => new IPEndPoint(IPAddress.Loopback, basePort + i)).ToList();
        _procs = new Process?[nodes + 1];
        _logs = new StreamWriter?[nodes + 1];
        _dataRoot = Path.GetFullPath(dataRoot);
        _echo = echo;
        _readMode = readMode;
        Directory.CreateDirectory(_dataRoot);
    }

    public List<IPEndPoint> Endpoints { get; }
    public int Count => Endpoints.Count;

    public int? Pid(int id) => _procs[id] is { HasExited: false } p ? p.Id : null;

    public void StartAll()
    {
        for (int id = 1; id <= Count; id++) Start(id);
    }

    public void Start(int id)
    {
        if (_procs[id] is { HasExited: false }) return;
        string exe = Environment.ProcessPath!;
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        if (Path.GetFileNameWithoutExtension(exe).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            psi.ArgumentList.Add(System.Reflection.Assembly.GetEntryAssembly()!.Location);
        foreach (var a in new[]
                 {
                     "serve", "--id", id.ToString(), "--cluster", string.Join(",", Endpoints.Select(e => e.ToString())),
                     "--data", Path.Combine(_dataRoot, $"n{id}"), "--read-mode", _readMode,
                     "--parent-pid", Environment.ProcessId.ToString(),
                 })
            psi.ArgumentList.Add(a);

        var p = Process.Start(psi) ?? throw new InvalidOperationException("could not start a node process");
        _logs[id] ??= new StreamWriter(Path.Combine(_dataRoot, $"n{id}.log"), append: true) { AutoFlush = true };
        var log = _logs[id]!;
        DataReceivedEventHandler pump = (_, e) =>
        {
            if (e.Data is null) return;
            lock (log) log.WriteLine(e.Data);
            if (_echo) Console.WriteLine($"[n{id}] {e.Data}");
        };
        p.OutputDataReceived += pump;
        p.ErrorDataReceived += pump;
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        _procs[id] = p;
    }

    /// <summary>Kills node <paramref name="id"/> by PID (no warning, like a crash).</summary>
    public void Kill(int id)
    {
        var p = _procs[id];
        if (p is null) return;
        try
        {
            if (!p.HasExited)
            {
                p.Kill(entireProcessTree: true);
                p.WaitForExit(5000);
            }
        }
        catch (InvalidOperationException) { }
        p.Dispose();
        _procs[id] = null;
    }

    public async Task<StatusResponse?[]> StatusAllAsync(TimeSpan timeout)
    {
        var tasks = Endpoints.Select(e => LatticeClient.StatusAsync(e, timeout)).ToArray();
        return await Task.WhenAll(tasks);
    }

    /// <summary>Waits until some running node reports itself leader; returns (id, term) or null on timeout.</summary>
    public async Task<(int Id, long Term)?> WaitForLeaderAsync(TimeSpan timeout, int excluding = 0, long minTerm = 0)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < timeout)
        {
            // Do not poll a node that was just killed: on Windows a connect to a closed local port waits for
            // the whole timeout, which would make every polling round slow.
            var polled = Endpoints.Where((_, i) => i + 1 != excluding).ToArray();
            var all = await Task.WhenAll(polled.Select(e => LatticeClient.StatusAsync(e, TimeSpan.FromMilliseconds(200))));
            var leader = all.Where(s => s is not null && s.Role == Role.Leader && s.Term > minTerm)
                .OrderByDescending(s => s!.Term).FirstOrDefault();
            if (leader is not null) return (leader.Id, leader.Term);
            await Task.Delay(2);
        }
        return null;
    }

    public void Dispose()
    {
        for (int id = 1; id <= Count; id++) Kill(id);
        foreach (var l in _logs) l?.Dispose();
    }
}
