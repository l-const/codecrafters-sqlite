const std = @import("std");

pub fn serialTypeToContentSize(serial_type: u64) u64 {
    if (serial_type >= 12 and serial_type % 2 == 0) {
        return (serial_type - 12) / 2;
    }

    if (serial_type >= 13 and serial_type % 2 == 1) {
        return (serial_type - 13) / 2;
    }

    if (serial_type == 0) {
        return 0; // NULL
    } else if (serial_type == 1) {
        return 1;
    } else if (serial_type == 2) {
        return 2;
    } else if (serial_type == 3) {
        return 3;
    } else if (serial_type == 4) {
        return 4;
    } else if (serial_type == 5) {
        return 6;
    } else if (serial_type == 6) {
        return 8;
    } else if (serial_type == 7) {
        return 8;
    } else if (serial_type == 8) {
        return 0; // INT(0)
    } else if (serial_type == 9) {
        return 0; // INT(1)
    } else if (serial_type == 10 or serial_type == 11) {
        return 0; // variable
    }
    return 0;
}

test "serialTypeToContentSize" {
    try std.testing.expectEqual(0, serialTypeToContentSize(0));
    try std.testing.expectEqual(1, serialTypeToContentSize(1));
    try std.testing.expectEqual(2, serialTypeToContentSize(2));
    try std.testing.expectEqual(3, serialTypeToContentSize(3));
    try std.testing.expectEqual(4, serialTypeToContentSize(4));
    try std.testing.expectEqual(6, serialTypeToContentSize(5));
    try std.testing.expectEqual(8, serialTypeToContentSize(6));
    try std.testing.expectEqual(8, serialTypeToContentSize(7));
    try std.testing.expectEqual(0, serialTypeToContentSize(8));
    try std.testing.expectEqual(0, serialTypeToContentSize(9));
    try std.testing.expectEqual(0, serialTypeToContentSize(10));
    try std.testing.expectEqual(0, serialTypeToContentSize(11));
    try std.testing.expectEqual(92, serialTypeToContentSize(196));
    try std.testing.expectEqual(7, serialTypeToContentSize(27));
}
