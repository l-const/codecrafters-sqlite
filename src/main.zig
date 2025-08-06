const std = @import("std");

const SQLITE_DEFAULT_PAGE_SIZE: u16 = 4096; // Default page size for SQLite
const SQLITE_HEADER_SIZE: u16 = 100; // Size of the SQLite header
const ROOT_CELL_SIZE_OFFSET: u16 = 3; // Offset for the root cell size in the header
const PAGE_HEADER_TABLE_INTERIOR_SIZE: u16 = 12; // Size of the page header
const PAGE_HEADER_TABLE_LEAF_SIZE: u16 = 8; // Size of the page header
const SQLITE_SCHEMA_TYPE_INDEX = 1;
const SQLITE_SCHEMA_NAME_INDEX = 2;
const SQLITE_SCHEMA_TYPE_TABLE_NAME_INDEX = 3;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
    } else {
        try std.io.getStdErr().writer().print("Unknown command: {s}\n", .{command});
    }
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
    var parser = Parser.init("CREATE TABLE", filePtr);
    // try parser.parse();
    const noOfTables = try parser.get_tables_count();
    _ = try parser.is_root_page();
    try stdOutWriter.print("number of tables: {d}\n", .{noOfTables});
}

fn tables(file: std.fs.File) !void {
    const allocator = std.heap.page_allocator;
    // try stdOutWriter.print("Listing tables is being implemented.\n", .{});
    const filePtr = @constCast(&file);
    var parser = Parser.init("CREATE TABLE", filePtr);
    const cellPointers = try parser.parse_cellpointer_array();
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
    std.debug.print("Tables found: {s}\n", .{filtered_names.items});
}

const TableCellType = enum {
    Interior,
    Leaf,
};

fn readPageRecords(file: *std.fs.File, cell_offsets: []u16, cellType: TableCellType) !std.ArrayList([]const u8) {
    // Implement the logic to read a record from the database file
    // based on the cell type (Interior or Leaf)

    const allocator = std.heap.page_allocator;
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
                const varint = try readVarInt(file);
                // std.debug.print("Leaf cell header varint: {d}, content size: {d}\n", .{ varint, serialTypeToContentSize(varint) });
                try headerVarInts.append(serialTypeToContentSize(varint));
                bytes_read += varint_byte_count(varint);
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

            @memcpy(schemaType, record_body_buf[0..schemaTypeSize]);
            // std.debug.print("Leaf cell schema type: {s}\n", .{schemaType});
            record_body_buf = std.mem.zeroes([256]u8);
            const schemaNameSize = headerVarInts.items[SQLITE_SCHEMA_NAME_INDEX - 1];
            _ = try file.read(record_body_buf[0..schemaNameSize]);
            const schemaName = try allocator.alloc(u8, schemaNameSize);
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

fn serialTypeToContentSize(serial_type: u64) u64 {
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

fn readVarInt(file: *std.fs.File) !u64 {
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

fn varint_byte_count(value: u64) u8 {
    var n: u8 = 1;
    var v = value;
    while (v >= 0x80) : (v >>= 7) {
        n += 1;
    }
    return n;
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
    count: u32,
    regex: []const u8,
    file: *std.fs.File,

    const Self = @This();

    pub fn init(regex: []const u8, file: *std.fs.File) Self {
        return Self{ .count = 0, .regex = regex, .file = file };
    }

    pub fn occurrences(self: Self) u32 {
        return self.count;
    }

    pub fn parse_cellpointer_array(self: *Self) !CellPointerArray {
        const allocator = std.heap.page_allocator;
        var cells_pointers = std.ArrayList(u16).init(allocator);

        try self.file.seekTo(SQLITE_HEADER_SIZE + ROOT_CELL_SIZE_OFFSET);
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

    pub fn parse(self: *Self) !void {
        const pageSize: u32 = 4096; // Example page size, adjust as needed
        var seekPos: u32 = 0;
        var buf: [1]u8 = undefined;
        try self.file.seekTo(seekPos);
        // Implement the parsing logic here
        // This is a placeholder for the actual implementation
        while (try self.file.read(&buf) != 0) {
            // Process the byte read from the file

            // std.debug.print("Read byte: {d}\n", .{buf[0]});
            if (buf[0] == self.regex[0]) {
                // Example condition to demonstrate parsing logic
                // std.debug.print("Found 'C' at position {}\n", .{seekPos});
                var regexRegex: [64]u8 = undefined;
                const n = try self.file.read(regexRegex[0 .. self.regex.len - 1]);
                if (std.mem.eql(u8, regexRegex[0..n], self.regex[1..])) {
                    // std.debug.print("Detected CREATE TABLE command\n", .{});
                    self.count += 1; // Example increment
                } else {
                    // std.debug.print("Detected other command: {s}\n", .{regexRegex});
                }
            }
            const currentPosition = try self.file.getPos();
            // std.debug.print("Current position in file: {}\n", .{currentPosition});

            if (currentPosition >= pageSize) {
                // std.debug.print("File has reached the end of the first file page - stop reading\n", .{});
                break; // Stop reading if we exceed a certain position
            }
            seekPos += 1;
        }
        // std.debug.print("Parsing regex appearances: {s}\n", .{self.regex});
    }
};
