const std = @import("std");

pub fn readVarInt(file: *std.fs.File) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    var buf: [1]u8 = undefined;

    while (true) {
        _ = try file.read(&buf);
        const byte = buf[0];
        result |= @as(u64, byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break; // MSB not set: last byte
        shift += 7;
        if (shift > 63) return error.Overflow; // Prevent overflow
    }

    // std.debug.print("Read varint: {d}\n", .{result});
    return result;
}

pub fn varint_byte_count(value: u64) u8 {
    var n: u8 = 1;
    var v = value;
    while (v >= 0x80) : (v >>= 7) {
        n += 1;
    }
    return n;
}
