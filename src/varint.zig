const std = @import("std");

pub fn readVarInt(file: *std.fs.File) !u64 {
    var buf: [9]u8 = undefined;
    var i: usize = 0;
    while (i < 9) {
        var tmp: [1]u8 = undefined;
        _ = try file.read(&tmp);
        buf[i] = tmp[0];
        if (i < 8 and (buf[i] & 0x80) == 0) {
            i += 1;
            break;
        }
        i += 1;
        if (i == 9) break;
    }
    if (i == 0) return error.UnexpectedEnd;
    if (i > 9) return error.Overflow;
    var result: u64 = 0;
    if (i < 9) {
        for (0..i) |j| {
            result = (result << 7) | @as(u64, buf[j] & 0x7F);
        }
    } else {
        // 9 bytes: first 8 use 7 bits, last uses all 8 bits
        for (0..8) |j| {
            result = (result << 7) | @as(u64, buf[j] & 0x7F);
        }
        result = (result << 8) | @as(u64, buf[8]);
    }
    // std.debug.print("Read varint: {d}, {any}\n", .{ result, buf[0..i] });
    return result;
}

pub fn varint_byte_count(value: u64) u8 {
    if (value <= 0x7f) return 1; // 1 byte for values <= 127
    if (value <= 0x3fff) return 2; // 2 bytes for values <= 16383
    var n: u8 = 1;
    var v = value;
    while (v >= 0x80) : (v >>= 7) {
        n += 1;
    }
    return n;
}

pub fn readVarIntFromSlice(slice: []const u8) !u64 {
    var buf: [9]u8 = undefined;
    var i: usize = 0;
    while (i < 9 and i < slice.len) {
        buf[i] = slice[i];
        if (i < 8 and (buf[i] & 0x80) == 0) {
            i += 1;
            break;
        }
        i += 1;
        if (i == 9) break;
    }
    if (i == 0) return error.UnexpectedEnd;
    if (i > 9) return error.Overflow;
    var result: u64 = 0;
    if (i < 9) {
        for (0..i) |j| {
            result = (result << 7) | @as(u64, buf[j] & 0x7F);
        }
    } else {
        for (0..8) |j| {
            result = (result << 7) | @as(u64, buf[j] & 0x7F);
        }
        result = (result << 8) | @as(u64, buf[8]);
    }
    return result;
}

// test
