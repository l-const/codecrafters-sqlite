const std = @import("std");
const readVarInt = @import("./varint.zig").readVarInt;
const varint_byte_count = @import("./varint.zig").varint_byte_count;
const serialTypeToContentSize = @import("./utils.zig").serialTypeToContentSize;

const SQLITE_DEFAULT_PAGE_SIZE: u16 = 4096; // Default page size for SQLite
const SQLITE_HEADER_SIZE: u16 = 100; // Size of the SQLite header
const ROOT_CELL_SIZE_OFFSET: u16 = 3; // Offset for the root cell size in the header
const PAGE_HEADER_TABLE_INTERIOR_SIZE: u16 = 12; // Size of the page header
const PAGE_HEADER_TABLE_LEAF_SIZE: u16 = 8; // Size of the page header
const SQLITE_SCHEMA_TYPE_INDEX = 1;
const SQLITE_SCHEMA_NAME_INDEX = 2;
const SQLITE_SCHEMA_TYPE_TABLE_NAME_INDEX = 3;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();
// const allocator = std.heap.c_allocator;

pub fn main() !void {
    // defer {
    //     const leak = gpa.deinit();
    //     switch (leak) {
    //         .leak => {
    //             @panic("Memory leak detected");
    //         },
    //         else => {},
    //     }
    // }

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        return;
    }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    if (std.mem.eql(u8, command, ".dbinfo")) {
        var file = try openDbFile(database_file_path);
        defer file.close();
        try dbInfo(file);
    } else if (std.mem.eql(u8, command, ".tables")) {
        var file = try openDbFile(database_file_path);
        defer file.close();
        try tables(file);
    } else if (std.mem.startsWith(u8, command, "SELECT ")) {
        var file = try openDbFile(database_file_path);
        defer file.close();
        try handle_query(file, command);
    } else {
        try std.io.getStdErr().writer().print("Unknown command: {s}\n", .{command});
    }
}

fn handle_query(_: std.fs.File, command: []const u8) !void {
    // Implement the logic to handle the query
    // This is a placeholder for the actual implementation
    var query_parts = std.mem.splitScalar(u8, command, ' ');
    var table_name: []const u8 = "";
    while (query_parts.next()) |part| {
        table_name = part;
    }

    std.debug.print("Query for table: {s}\n", .{table_name});
    std.debug.print("Query handling is not implemented yet.\n", .{});
}

fn find_table_root_page() !void {
    // Implement the logic to find the root page of a table
    // This is a placeholder for the actual implementation
    std.debug.print("Finding table root page is not implemented yet.\n", .{});
}

fn openDbFile(file_path: []const u8) !std.fs.File {
    const file = try std.fs.cwd().openFile(file_path, .{});
    return file;
}

fn dbInfo(file: std.fs.File) !void {
    // Implement the logic to gather database information
    // Uncomment this block to pass the first stage
    var buf: [2]u8 = undefined;
    _ = try file.seekTo(16);
    _ = try file.read(&buf);
    const page_size = std.mem.readInt(u16, &buf, .big);
    var stdOutWriter = std.io.getStdOut().writer();
    try stdOutWriter.print("database page size: {}\n", .{page_size});
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    // try parser.parse();
    const noOfTables = try parser.get_tables_count();
    _ = try parser.is_root_page();
    try stdOutWriter.print("number of tables: {d}\n", .{noOfTables});
}

fn tables(file: std.fs.File) !void {
    // try stdOutWriter.print("Listing tables is being implemented.\n", .{});
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    const cellPointers = try parser.parse_cellpointer_array(1);
    defer cellPointers.deinit();
    // std.debug.print("Cell pointers: size: {d}, page: {d} , items[1]: {x}, items: {any}\n", .{ cellPointers.size, cellPointers.pageNo, try cellPointers.get_nth_offset(1), cellPointers.get_cells_pointers() });
    const table_names = try readPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);

    defer table_names.deinit();
    var filtered_names = std.ArrayList([]const u8).init(allocator);
    defer filtered_names.deinit();
    for (table_names.items) |name| {
        if (!std.mem.startsWith(u8, name, "sqlite")) {
            try filtered_names.append(name);
        }
    }
    defer {
        for (table_names.items) |name| {
            allocator.free(name); // manually free each heap-allocated item
        }
    }
    const stdOutWriter = std.io.getStdOut().writer();
    for (filtered_names.items) |name| {
        try stdOutWriter.print("{s} ", .{name});
    }
    try stdOutWriter.print("\n", .{});
}

const TableCellType = enum {
    Interior,
    Leaf,
};

fn readPageRecords(file: *std.fs.File, cell_offsets: []u16, cellType: TableCellType) !std.ArrayList([]const u8) {
    // Implement the logic to read a record from the database file
    // based on the cell type (Interior or Leaf)

    var table_names = std.ArrayList([]const u8).init(allocator);
    for (cell_offsets) |offset| {
        try file.seekTo(offset);
        // _ = try file.read(&buf);
        // const cellSize = std.mem.readInt(u16, &buf, .big);
        if (cellType == TableCellType.Leaf) {
            const payload_size = try readVarInt(file);
            // std.debug.print("Leaf cell varint payload: {d}\n", .{payload_size});
            const rowId = try readVarInt(file);
            // std.debug.print("Leaf cell row ID: {d}\n", .{rowId});
            // Read the rest of the leaf cell data
            // Read leaf cell data
            // std.debug.print("Leaf cell data: {s}\n", .{"Payload "});
            const record_header_size = try readVarInt(file);
            // std.debug.print("Leaf cell record header size: {d}\n", .{record_header_size});
            const record_header_field_bytes = varint_byte_count(record_header_size);
            // std.debug.print("Leaf cell record header field bytes: {d}\n", .{record_header_field_bytes});
            // const seek_to_record_body_bytes = record_header_size  - record_header_field_bytes;
            const payload_size_offset = varint_byte_count(payload_size);
            const row_id_size_bytes = varint_byte_count(rowId);

            const offset_for_record_header = offset + payload_size_offset + row_id_size_bytes + record_header_field_bytes;
            // std.debug.print("Leaf cell seek to offset: {d}\n", .{offset_for_record_header});
            try file.seekTo(offset_for_record_header);
            var headerVarInts = std.ArrayList(u64).init(allocator);
            defer headerVarInts.deinit();
            var bytes_read: u16 = 0;
            while (bytes_read < record_header_size) {
                const varInt = try readVarInt(file);
                // std.debug.print("Leaf cell header varint: {d}, content size: {d}\n", .{ varInt, serialTypeToContentSize(varInt) });
                try headerVarInts.append(serialTypeToContentSize(varInt));
                bytes_read += varint_byte_count(varInt);
            }
            const offset_for_record_body = offset + payload_size_offset + row_id_size_bytes + record_header_size;
            // std.debug.print("Leaf cell seek to offset: {d}\n", .{offset_for_record_body});
            try file.seekTo(offset_for_record_body);
            var record_body_buf: [256]u8 = undefined; // Adjust size as needed
            _ = try file.read(record_body_buf[0..payload_size]);
            record_body_buf = std.mem.zeroes([256]u8);
            try file.seekTo(offset_for_record_body);
            const schemaTypeSize = headerVarInts.items[SQLITE_SCHEMA_TYPE_INDEX - 1];
            _ = try file.read(record_body_buf[0..schemaTypeSize]);
            const schemaType = try allocator.alloc(u8, schemaTypeSize);
            defer allocator.free(schemaType);
            @memcpy(schemaType, record_body_buf[0..schemaTypeSize]);

            // std.debug.print("Leaf cell schema type: {s}\n", .{schemaType});
            record_body_buf = std.mem.zeroes([256]u8);
            const schemaNameSize = headerVarInts.items[SQLITE_SCHEMA_NAME_INDEX - 1];
            _ = try file.read(record_body_buf[0..schemaNameSize]);
            const schemaName = try allocator.alloc(u8, schemaNameSize);
            defer allocator.free(schemaName);
            @memcpy(schemaName, record_body_buf[0..schemaNameSize]);
            // std.debug.print("Leaf cell schema name: {s}\n", .{schemaName});
            record_body_buf = std.mem.zeroes([256]u8);
            const tableNameSize = headerVarInts.items[SQLITE_SCHEMA_TYPE_TABLE_NAME_INDEX - 1];
            _ = try file.read(record_body_buf[0..tableNameSize]);
            const tableName = try allocator.alloc(u8, tableNameSize);
            @memcpy(tableName, record_body_buf[0..tableNameSize]);
            // std.debug.print("Leaf cell table name: {s}\n", .{tableName});
            try table_names.append(tableName);
        } else {
            // Read interior cell data
            std.debug.print("Interior cell data: {any}\n", .{"not done yet"});
        }
    }
    return table_names;
}

// Define the CellPointerArray structure
const CellPointerArrayError = error{
    OutOfBounds,
};

const CellPointerArray = struct {
    size: usize,
    cells_pointers: std.ArrayList(u16), // Using ArrayList for dynamic size
    pageNo: u32 = 0, // Current page number

    const Self = @This();

    pub fn init(cells_pointers: std.ArrayList(u16)) Self {
        const cellsPtr = @constCast(&cells_pointers);
        const pageNo = Self.current_page_number(cellsPtr);
        return Self{ .size = cells_pointers.items.len, .cells_pointers = cells_pointers, .pageNo = pageNo };
    }

    pub fn deinit(self: Self) void {
        // Cleanup if necessary
        self.cells_pointers.deinit();
    }

    pub inline fn get_size(self: *Self) usize {
        return self.size;
    }

    pub inline fn get_page_number(self: *Self) u32 {
        return self.pageNo;
    }

    inline fn current_page_number(cel_pointers: *std.ArrayList(u16)) u32 {
        const offset = cel_pointers.items[1]; // Get the first cell pointer or return 0 if empty
        // Round up division
        return (offset + SQLITE_DEFAULT_PAGE_SIZE - 1) / SQLITE_DEFAULT_PAGE_SIZE;
    }

    pub fn get_cells_pointers(self: Self) []u16 {
        return self.cells_pointers.items;
    }

    /// Returns the nth cell pointer from the array.
    /// 1 is the first cell pointer.
    pub fn get_nth_offset(self: Self, n: u32) CellPointerArrayError!u16 {
        if (n > self.size) {
            return CellPointerArrayError.OutOfBounds; // or handle error
        }
        return self.cells_pointers.items[n - 1];
    }
};

const Parser = struct {
    // Define your parser structure here
    // This is a placeholder for the actual implementation
    file: *std.fs.File,

    const Self = @This();

    pub fn init(file: *std.fs.File) Self {
        return Self{ .file = file };
    }

    pub fn parse_cellpointer_array(self: *Self, pageNumber: u32) !CellPointerArray {
        var cells_pointers = std.ArrayList(u16).init(allocator);
        if (pageNumber > 1) {
            try self.file.seekTo(pageNumber * SQLITE_DEFAULT_PAGE_SIZE + ROOT_CELL_SIZE_OFFSET);
        } else {
            try self.file.seekTo(SQLITE_HEADER_SIZE + ROOT_CELL_SIZE_OFFSET);
        }
        var buf: [2]u8 = undefined;
        _ = try self.file.read(&buf);
        const cellsCount = std.mem.readInt(u16, &buf, .big);
        try self.file.seekTo(SQLITE_HEADER_SIZE + PAGE_HEADER_TABLE_LEAF_SIZE);
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
};
