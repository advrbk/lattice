namespace Lattice.Util;

internal static class BinaryExtensions
{
    public static void WriteNullable(this BinaryWriter w, string? s)
    {
        w.Write(s is not null);
        if (s is not null) w.Write(s);
    }

    public static string? ReadNullableString(this BinaryReader r) => r.ReadBoolean() ? r.ReadString() : null;

    public static void WriteBytes(this BinaryWriter w, byte[] data)
    {
        w.Write(data.Length);
        w.Write(data);
    }

    public static byte[] ReadBytes32(this BinaryReader r)
    {
        int n = r.ReadInt32();
        if (n < 0) throw new InvalidDataException("negative length");
        byte[] b = r.ReadBytes(n);
        if (b.Length != n) throw new EndOfStreamException();
        return b;
    }
}
