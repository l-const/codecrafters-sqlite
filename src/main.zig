const std = @import("std");
const Parser = @import("./parser.zig");
const globals = @import("./globals.zig");
const sqlparser = @import("./sql/parser.zig");
const Buffer = @import("./types.zig").Buffer;
const PageContent = @import("./types.zig").PageContent;
const readVarInt = @import("./varint.zig").readVarInt;
const varint_byte_count = @import("./varint.zig").varint_byte_count;
const serialTypeToContentSize = @import("./utils.zig").serialTypeToContentSize;
const allocator = globals.allocator;

pub fn main() !void {
    const args = try std.process.argsAlloc(allocator);
    defer globals.arena_instance.deinit(); // Deinit the global arena allocator at program exit
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        return;
    }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    try run(database_file_path, command);
}

fn run(db_path: []const u8, command: []const u8) !void {
    var file = try openDbFile(db_path);
    defer file.close();
    if (std.mem.eql(u8, command, ".dbinfo")) {
        try dbInfo(file);
    } else if (std.mem.eql(u8, command, ".tables")) {
        try tables(file);
    } else if (std.mem.eql(u8, command, ".schema")) {
        try schema(file);
    } else if (std.mem.startsWith(u8, command, "SELECT ") or std.mem.startsWith(u8, command, "select ")) {
        try handle_query(file, command);
    } else {
        try std.io.getStdErr().writer().print("Unknown command: {s}\n", .{command});
    }
}

test "Run command .dbinfo with a valid database file" {
    const db_path = "sample.db"; // Replace with a valid database file path
    const command = ".dbinfo"; // Replace with a valid command
    try run(db_path, command);
}

test "Run command .tables with a valid database file" {
    const db_path = "sample.db"; // Replace with a valid database file path
    const command = ".tables"; // Replace with a valid command
    try run(db_path, command);
}

test "Run command .schema with a valid database file" {
    const db_path = "sample.db"; // Replace with a valid database file path
    const command = ".schema"; // Replace with a valid command
    try run(db_path, command);
}

test "Run command with a valid database file and query" {
    const db_path = "sample.db"; // Replace with a valid database file path
    const command = "SELECT name FROM users"; // Replace with a valid SQL query
    try run(db_path, command);
}

fn handle_query(file: std.fs.File, command: []const u8) !void {
    var sqlParser = try sqlparser.SQLParser.init(command, allocator);
    defer sqlParser.deinit();
    var statement = try sqlParser.parse(null);
    defer statement.deinit();
    if (statement != sqlparser.StatementType.Select) {
        try std.io.getStdErr().writer().print("Only SELECT statements are supported for now.\n", .{});
        return;
    }
    const table_name = statement.Select.table;
    const duped_table_name = table_name;
    var lowercase: []u8 = try allocator.alloc(u8, table_name.len);
    defer allocator.free(lowercase);
    lowercase = std.ascii.lowerString(lowercase, duped_table_name);
    const rootPage = try find_table_root_page(file, lowercase);
    var parser = Parser.init(@constCast(&file));
    const cellPointers = try parser.parse_cellpointer_array(rootPage);
    const rows = cellPointers.get_size();
    if (statement.Select.is_count) {
        try std.io.getStdOut().writer().print("{d}\n", .{rows});
        return;
    }
    const filerootPageOffset = rootPage * globals.SQLITE_DEFAULT_PAGE_SIZE;
    try file.seekTo(filerootPageOffset);
    var buf: [globals.SQLITE_DEFAULT_PAGE_SIZE]u8 = undefined;
    _ = try file.read(&buf);
    var root_page_content = try Buffer.init(&buf, allocator);
    defer root_page_content.deinit();
    var page_content = PageContent{
        .offset = 0,
        .buffer = root_page_content,
    };
    defer page_content.deinit();
    parse_page_rows(page_content);
}

fn parse_page_rows(page_content: PageContent) void {
    // TODO: Implement row parsing logic
    std.debug.print("page_content offset: {}\n", .{page_content.offset});
}

fn find_table_root_page(file: std.fs.File, table_name: []const u8) !u32 {
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    const cellPointers = try parser.parse_cellpointer_array(1);
    defer cellPointers.deinit();
    const tableRootPages =
        try readRootPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);
    const table_names = tableRootPages.tables;
    const root_pages = tableRootPages.rootPages;
    var index: ?usize = null;
    for (table_names.items, 0..) |item, i| {
        if (std.mem.eql(u8, item, table_name)) {
            index = i;
            break;
        }
    }
    if (index == null) return 0;
    return root_pages.items[index.?];
}

fn openDbFile(file_path: []const u8) !std.fs.File {
    const file = try std.fs.cwd().openFile(file_path, .{});
    return file;
}

fn schema(file: std.fs.File) !void {
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    const cellPointers = try parser.parse_cellpointer_array(1);
    defer cellPointers.deinit();
    const tableRootPages =
        try readRootPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);
    var table_names = tableRootPages.tables;
    defer table_names.deinit();
    for (tableRootPages.sqls.items) |sql| {
        try std.io.getStdOut().writer().print("{s};\n", .{sql});
    }
}

fn dbInfo(file: std.fs.File) !void {
    _ = try file.seekTo(0);
    var buf: [globals.SQLITE_DEFAULT_PAGE_SIZE]u8 = undefined;
    _ = try file.read(&buf);
    var root_page_content = try Buffer.init(&buf, allocator);
    defer root_page_content.deinit();
    var page_content = PageContent{
        .offset = 100,
        .buffer = root_page_content,
    };
    defer page_content.deinit();
    const page_size = page_content.read_u16_at(16);
    var stdOutWriter = std.io.getStdOut().writer();
    try stdOutWriter.print("database page size: {}\n", .{page_size});
    const noOfTables = page_content.read_u16(globals.ROOT_CELL_SIZE_OFFSET);
    try stdOutWriter.print("number of tables: {d}\n", .{noOfTables});
}

fn tables(file: std.fs.File) !void {
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    const cellPointers = try parser.parse_cellpointer_array(1);
    defer cellPointers.deinit();
    const tableRootPages =
        try readRootPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);
    var table_names = tableRootPages.tables;
    var sql_schemas = tableRootPages.sqls;
    defer table_names.deinit();
    defer sql_schemas.deinit();
    var filtered_names = std.ArrayList([]const u8).init(allocator);
    defer filtered_names.deinit();
    for (table_names.items) |name| {
        if (!std.mem.startsWith(u8, name, "sqlite")) {
            try filtered_names.append(name);
        }
    }
    defer {
        for (table_names.items) |name| {
            allocator.free(name);
        }
    }
    defer {
        for (sql_schemas.items) |sql| {
            allocator.free(sql);
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

const TableRootPages = struct {
    tables: std.ArrayList([]const u8), // List of table names
    sqls: std.ArrayList([]const u8), // List of SQL CREATE statements - SCHEMAS
    rootPages: std.ArrayList(u32), // List of root page numbers
};

fn readRecords(page_content: PageContent) void {
    // Implement the logic to read records from the database file
    // This is a placeholder for the actual implementation
    const page_type = page_content.getPageType();
    const is_leaf = page_type.is_leaf();
    const is_table = page_type.is_table();
    std.debug.print("Reading records for page type: is_leaf: {any}, is_table: {any} \n", .{ is_leaf, is_table });
}

fn readRootPageRecords(file: *std.fs.File, cell_offsets: []u16, cellType: TableCellType) !TableRootPages {
    var table_names = std.ArrayList([]const u8).init(allocator);
    var root_pages = std.ArrayList(u32).init(allocator);
    var sql_schemas = std.ArrayList([]const u8).init(allocator);
    for (cell_offsets) |offset| {
        try file.seekTo(offset);
        if (cellType == TableCellType.Leaf) {
            const payload_size = try readVarInt(file);
            const rowId = try readVarInt(file);
            // Read the rest of the leaf cell data
            // Read leaf cell data
            // std.debug.print("Leaf cell data: {s}\n", .{"Payload "});
            const record_header_size = try readVarInt(file);
            const record_header_field_bytes = varint_byte_count(record_header_size);
            const payload_size_offset = varint_byte_count(payload_size);
            const row_id_size_bytes = varint_byte_count(rowId);

            const offset_for_record_header = offset + payload_size_offset + row_id_size_bytes + record_header_field_bytes;
            // std.debug.print("Offset for record header: {d}\n", .{offset_for_record_header});
            try file.seekTo(offset_for_record_header);
            var headerVarInts = std.ArrayList(u64).init(allocator);
            var bytes_read: u16 = 0;
            while (bytes_read < record_header_size) {
                const varInt = try readVarInt(file);
                try headerVarInts.append(serialTypeToContentSize(varInt));
                // std.debug.print("Leaf cell header varint: {d}\n", .{varInt});
                // std.debug.print("Leaf cell header varint size: {d}\n", .{varint_byte_count(varInt)});
                bytes_read += varint_byte_count(varInt);
            }
            const offset_for_record_body = offset + payload_size_offset + row_id_size_bytes + record_header_size;
            // std.debug.print("Offset for record body: {d}\n", .{offset_for_record_body});
            try file.seekTo(offset_for_record_body);
            var record_body_buf: [256]u8 = undefined; // Adjust size as needed
            _ = try file.read(record_body_buf[0..payload_size]);
            record_body_buf = std.mem.zeroes([256]u8);
            try file.seekTo(offset_for_record_body);
            const schemaTypeSize = headerVarInts.items[globals.SQLITE_SCHEMA_TYPE_INDEX - 1];
            _ = try file.read(record_body_buf[0..schemaTypeSize]);
            const schemaType = try allocator.alloc(u8, schemaTypeSize);
            defer allocator.free(schemaType);
            @memcpy(schemaType, record_body_buf[0..schemaTypeSize]);

            // std.debug.print("Leaf cell schema type: {s}\n", .{schemaType});
            record_body_buf = std.mem.zeroes([256]u8);
            const schemaNameSize = headerVarInts.items[globals.SQLITE_SCHEMA_NAME_INDEX - 1];
            _ = try file.read(record_body_buf[0..schemaNameSize]);
            const schemaName = try allocator.alloc(u8, schemaNameSize);
            defer allocator.free(schemaName);
            @memcpy(schemaName, record_body_buf[0..schemaNameSize]);
            // std.debug.print("Leaf cell schema name: {s}\n", .{schemaName});
            record_body_buf = std.mem.zeroes([256]u8);
            const tableNameSize = headerVarInts.items[globals.SQLITE_SCHEMA_TYPE_TABLE_NAME_INDEX - 1];
            _ = try file.read(record_body_buf[0..tableNameSize]);
            const tableName = try allocator.alloc(u8, tableNameSize);
            @memcpy(tableName, record_body_buf[0..tableNameSize]);
            // std.debug.print("Leaf cell table name: {s}\n", .{tableName});
            try table_names.append(tableName);
            record_body_buf = std.mem.zeroes([256]u8);
            const rootPageSize = headerVarInts.items[globals.SQLITE_SCHEMA_TYPE_TABLE_ROOT_PAGE_INDEX - 1];
            _ = try file.read(record_body_buf[0..rootPageSize]);
            var rootSlice: [4]u8 = undefined;
            for (rootSlice, 0..rootSlice.len) |_, i| {
                rootSlice[i] = record_body_buf[i];
            }
            const rootPage: u32 = std.mem.bytesToValue(u32, rootSlice[0..4]);
            // std.debug.print("Leaf cell root page: {d}\n", .{rootPage});
            try root_pages.append(rootPage);
            record_body_buf = std.mem.zeroes([256]u8);
            const sqlSize = headerVarInts.items[globals.SQLITE_SCHEMA_TYPE_TABLE_SCHEMA_INDEX - 1];
            _ = try file.read(record_body_buf[0..sqlSize]);
            const sqlSchema = try allocator.alloc(u8, sqlSize);
            // defer allocator.free(sqlSchema);
            @memcpy(sqlSchema, record_body_buf[0..sqlSize]);
            // std.debug.print("Leaf cell SQL schema: {s}\n", .{sqlSchema});
            try sql_schemas.append(sqlSchema);
        } else {
            // Read interior cell data
            std.debug.print("Interior cell data: {any}\n", .{"not done yet"});
        }
    }
    return .{ .tables = table_names, .sqls = sql_schemas, .rootPages = root_pages };
}
