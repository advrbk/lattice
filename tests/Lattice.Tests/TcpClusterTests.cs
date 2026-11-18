using System.Net;
using System.Net.Sockets;
using Lattice.Net;
using Lattice.Raft;

namespace Lattice.Tests;

/// <summary>
/// A smoke test of the real TCP transport and file storage: three in-process servers on loopback.
/// Correctness is tested in the simulator; this only checks that the pieces work over real sockets,
/// with generous timeouts so machine load cannot make it flaky.
/// </summary>
public class TcpClusterTests
{
    private static List<IPEndPoint> FreeEndpoints(int n)
    {
        var listeners = Enumerable.Range(0, n).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToList();
        listeners.ForEach(l => l.Start());
        var endpoints = listeners.Select(l => (IPEndPoint)l.LocalEndpoint).ToList();
        listeners.ForEach(l => l.Stop());
        return endpoints;
    }

    [Fact]
    public async Task Three_node_cluster_over_tcp_survives_losing_its_leader()
    {
        using var dir = new TempDir();
        var endpoints = FreeEndpoints(3);
        var hosts = new ServerHost?[4];
        ServerHost StartHost(int id)
        {
            var h = new ServerHost(new ServerConfig
            {
                Id = id, Cluster = endpoints, DataDirectory = Path.Combine(dir.Path, $"n{id}"),
            });
            h.Start();
            return h;
        }
        try
        {
            for (int id = 1; id <= 3; id++) hosts[id] = StartHost(id);
            await using var client = new LatticeClient(endpoints) { Timeout = TimeSpan.FromSeconds(30) };

            var put = await client.PutAsync("color", "blue");
            Assert.True(put.Success);
            Assert.Equal("blue", (await client.GetAsync("color")).Value);
            Assert.False((await client.CasAsync("color", "red", "green")).Success);
            Assert.True((await client.CasAsync("color", "blue", "green")).Success);
            Assert.True((await client.DeleteAsync("color")).Success);
            Assert.Null((await client.GetAsync("color")).Value);
            await client.PutAsync("color", "violet");

            // Stop the leader; the other two elect a new one and keep serving.
            int leader = put.Leader;
            var status = await LatticeClient.StatusAsync(endpoints[leader - 1], TimeSpan.FromSeconds(5));
            Assert.Equal(Role.Leader, status!.Role);
            hosts[leader]!.Dispose();
            hosts[leader] = null;

            Assert.Equal("violet", (await client.GetAsync("color")).Value);
            await client.PutAsync("after", "failover");

            // Restart it from its data directory: it rejoins and catches up.
            hosts[leader] = StartHost(leader);
            await client.PutAsync("rejoined", "yes");
            var deadline = DateTime.UtcNow.AddSeconds(30);
            StatusResponse? s = null;
            while (DateTime.UtcNow < deadline)
            {
                s = await LatticeClient.StatusAsync(endpoints[leader - 1], TimeSpan.FromSeconds(2));
                if (s is not null && s.Keys == 3) break;
                await Task.Delay(50);
            }
            Assert.Equal(3, s!.Keys);
        }
        finally
        {
            foreach (var h in hosts) h?.Dispose();
        }
    }
}
