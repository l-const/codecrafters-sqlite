const std = @import("std");
const allocator = @import("./globals.zig").allocator;
const ROOT_CELL_SIZE_OFFSET: u16 = @import("./globals.zig").ROOT_CELL_SIZE_OFFSET;
const SQLITE_DEFAULT_PAGE_SIZE = @import("./globals.zig").SQLITE_DEFAULT_PAGE_SIZE;
const SQLITE_HEADER_SIZE: u16 = @import("./globals.zig").SQLITE_HEADER_SIZE;
const PAGE_HEADER_TABLE_LEAF_SIZE: u16 = @import("./globals.zig").PAGE_HEADER_TABLE_LEAF_SIZE;
const PAGE_HEADER_TABLE_INTERIOR_SIZE: u16 = @import("./globals.zig").PAGE_HEADER_TABLE_INTERIOR_SIZE;
const CellPointerArray = @import("./types.zig").CellPointerArray;
// Define your parser structure here
// This is a placeholder for the actual implementation
file: *std.fs.File,

const Self = @This();

pub fn init(file: *std.fs.File) Self {
    return Self{ .file = file };
}

pub fn parse_cellpointer_array(self: *Self, pageNumber: u32) !CellPointerArray {
    var cells_pointers = std.ArrayList(u16).init(allocator);
    const seekToOffset = if (pageNumber > 1)
        (pageNumber - 1) * SQLITE_DEFAULT_PAGE_SIZE + ROOT_CELL_SIZE_OFFSET
    else
        SQLITE_HEADER_SIZE + ROOT_CELL_SIZE_OFFSET;
    try self.file.seekTo(seekToOffset);
    // std.debug.print("Seeking to offset: {d}\n", .{seekToOffset});
    // Read the number of cells in the page
    var buf: [2]u8 = undefined;
    _ = try self.file.read(&buf);
    const cellsCount = std.mem.readInt(u16, &buf, .big);
    // std.debug.print("Number of cells in page {d}: {d}\n", .{ pageNumber, cellsCount });
    const leafHeaderOffset = if (pageNumber > 1)
        (pageNumber - 1) * SQLITE_DEFAULT_PAGE_SIZE + PAGE_HEADER_TABLE_LEAF_SIZE
    else
        SQLITE_HEADER_SIZE + PAGE_HEADER_TABLE_LEAF_SIZE;
    try self.file.seekTo(leafHeaderOffset);
    for (0..cellsCount) |_| {
        _ = try self.file.read(&buf);
        const cellOffset = std.mem.readInt(u16, &buf, .big);
        try cells_pointers.append(cellOffset);
    }

    return CellPointerArray.init(cells_pointers);
}

pub fn is_root_page(self: *Self) !bool {
    // Check if the current page is the root page
    const filePos = try self.file.getPos();
    std.debug.print("Current file position: {}\n", .{filePos});
    return filePos <= SQLITE_DEFAULT_PAGE_SIZE;
}

pub fn get_tables_count(self: *Self) !u32 {
    self.file.seekTo(SQLITE_HEADER_SIZE + ROOT_CELL_SIZE_OFFSET) catch |err| {
        std.debug.print("Error seeking to root cell size offset: {}\n", .{err});
        return 0;
    };
    var buf: [2]u8 = undefined;
    _ = try self.file.read(&buf);
    const tablesCount = std.mem.readInt(u16, &buf, .big);
    return tablesCount;
}
