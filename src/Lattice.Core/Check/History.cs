namespace Lattice.Check;

public enum OpKind { Get, Put, Delete, Cas }

/// <summary>
/// One client operation as seen from outside: when it was invoked, when (if ever) the client saw a
/// definite answer, and what that answer was. Times only need to be ordered, so the simulator uses a
/// global counter and the real cluster uses Stopwatch ticks.
/// </summary>
public sealed class Operation
{
    public int Id { get; set; }
    public int Client { get; init; }
    public required string Key { get; init; }
    public OpKind Kind { get; init; }

    /// <summary>Put: the value written. Cas: the new value.</summary>
    public string? Value { get; init; }

    /// <summary>Cas: the value the key must hold (null = absent).</summary>
    public string? Expected { get; init; }

    public long Invoke { get; set; }

    /// <summary>
    /// When the client saw the answer, or <see cref="Unknown"/> if it never did (timeout, crash).
    /// An operation with an unknown outcome may have taken effect at any point after its invocation, or never.
    /// </summary>
    public long Return { get; set; } = Unknown;

    /// <summary>Get: the value read. Failed cas: the value found instead of the expected one. Null = absent.</summary>
    public string? Output { get; set; }

    /// <summary>Cas: whether it reported success.</summary>
    public bool CasSucceeded { get; set; }

    public bool Completed => Return != Unknown;

    public const long Unknown = long.MaxValue;

    public override string ToString()
    {
        string body = Kind switch
        {
            OpKind.Get => $"get({Key}) -> {(Completed ? Show(Output) : "?")}",
            OpKind.Put => $"put({Key}, {Show(Value)})",
            OpKind.Delete => $"delete({Key})",
            _ => $"cas({Key}, {Show(Expected)} -> {Show(Value)}) -> {(Completed ? CasSucceeded ? "ok" : $"failed, found {Show(Output)}" : "?")}",
        };
        string ret = Completed ? Return.ToString() : "never";
        return $"#{Id} client {Client}: {body}  [{Invoke}, {ret}]";
    }

    private static string Show(string? s) => s is null ? "nil" : $"\"{s}\"";

    public static Operation Get(int client, string key, string? output, long invoke, long ret) =>
        new() { Client = client, Key = key, Kind = OpKind.Get, Output = output, Invoke = invoke, Return = ret };

    public static Operation Put(int client, string key, string value, long invoke, long ret) =>
        new() { Client = client, Key = key, Kind = OpKind.Put, Value = value, Invoke = invoke, Return = ret };

    public static Operation Delete(int client, string key, long invoke, long ret) =>
        new() { Client = client, Key = key, Kind = OpKind.Delete, Invoke = invoke, Return = ret };

    /// <summary>A cas. If it failed, <paramref name="found"/> is the value it reported finding.</summary>
    public static Operation Cas(int client, string key, string? expected, string? value, bool ok, long invoke, long ret, string? found = null) =>
        new() { Client = client, Key = key, Kind = OpKind.Cas, Expected = expected, Value = value, CasSucceeded = ok, Output = ok ? null : found, Invoke = invoke, Return = ret };
}
