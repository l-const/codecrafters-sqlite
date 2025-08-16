const std = @import("std");
const Parser = @import("./parser.zig");
const globals = @import("./globals.zig");
const sqlparser = @import("./sql/parser.zig");
const Column = sqlparser.Column;
const Buffer = @import("./types.zig").Buffer;
const Row = @import("./types.zig").Row;
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
    const rootPageSql = try find_table_root_page(file, lowercase);
    const rootPage = rootPageSql.root_page;
    const table_schema = rootPageSql.sql;
    var parser = Parser.init(@constCast(&file));
    const cellPointers = try parser.parse_cellpointer_array(rootPage);
    const rows = cellPointers.get_size();
    if (statement.Select.is_count) {
        try std.io.getStdOut().writer().print("{d}\n", .{rows});
        return;
    }
    // Find columns in the select statement if any
    const select_columns = statement.Select.columns;
    // std.debug.print("Column(s) in select: ", .{});
    // for (select_columns.items) |column| {
    //     std.debug.print("{s} ", .{column});
    // }
    // std.debug.print("\n", .{});

    sqlParser = try sqlparser.SQLParser.init(table_schema, allocator);
    const stmt = try sqlParser.parse(null);
    const create_stmt = stmt.CreateTable;
    const table_columns: std.ArrayList(Column) = create_stmt.columns;
    const filerootPageOffset = (rootPage - 1) * globals.SQLITE_DEFAULT_PAGE_SIZE;
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
    var where_clause: ?sqlparser.WhereClause = null;

    if (statement.Select.where_clause) |where| {
        where_clause = where;
    }
    try parse_page_rows(page_content, table_columns.items, select_columns.items, where_clause);
}

fn filter_rows(rows: []Row, where_clause: sqlparser.WhereClause, schema_columns: []const Column) ![]Row {
    // This function is a placeholder for filtering rows based on the WHERE clause.
    // The actual implementation will depend on the structure of the WHERE clause
    // and how it should be applied to the rows.
    // For now, it does nothing.
    // find index of where_clause column in schema_columns
    var where_column_index: ?usize = null;
    for (schema_columns, 0..schema_columns.len) |col, idx| {
        if (std.mem.eql(u8, col.name, where_clause.column)) {
            where_column_index = idx;
            break;
        }
    }
    if (where_column_index == null) return error.OutOfBounds;

    var filtered_rows = std.ArrayList(Row).init(allocator);
    // Filter rows based on the WHERE clause
    for (rows) |row| {
        const value = row.fields[where_column_index.?];
        if (where_clause.operator == sqlparser.Operator.Equal) {
            if (!std.mem.eql(u8, value, where_clause.value)) {
                // Skip this row
                continue;
            }
        } else if (where_clause.operator == sqlparser.Operator.NotEqual) {
            if (std.mem.eql(u8, value, where_clause.value)) {
                // Skip this row
                continue;
            }
        }
        // If we reach here, the row matches the WHERE clause
        // Do something with the matching row
        // return row; // This is just a placeholder
        try filtered_rows.append(row);
    }
    return filtered_rows.toOwnedSlice();
}

fn parse_page_rows(page_content: PageContent, table_columns: []const Column, select_columns: [][]const u8, where_clause: ?sqlparser.WhereClause) !void {
    const rowsList = try page_content.getRows();
    var rows = rowsList.items;

    // If a WHERE clause is provided, filter the rows based on it
    if (where_clause) |where| {
        rows = try filter_rows(rows, where, table_columns);
    }

    // Build a list of column names
    var column_names = std.ArrayList([]const u8).init(allocator);
    defer column_names.deinit();
    for (table_columns) |column| {
        try column_names.append(column.name);
    }

    // Find the indexes of all select_columns in column_names
    var columnIndexs = std.ArrayList(u64).init(allocator);
    defer columnIndexs.deinit();
    for (select_columns) |sel_col| {
        var found: bool = false;
        for (column_names.items, 0..) |col_name, idx| {
            if (std.mem.eql(u8, sel_col, col_name)) {
                try columnIndexs.append(idx);
                found = true;
                break;
            }
        }
        if (!found) {
            std.debug.print("Selected column not found: {s}\n", .{sel_col});
        }
    }
    if (columnIndexs.items.len == 0) {
        std.debug.print("No valid selected columns found\n", .{});
        return;
    }

    // Use all found indexes in format_output_row
    for (rows) |row| {
        try format_output_row(row, columnIndexs.items);
    }
}

fn format_output_row(row: Row, columnIndexs: []u64) !void {
    if (columnIndexs.len > 0) {
        std.debug.assert(columnIndexs.len > 0);
    }
    const stdOutWriter = std.io.getStdOut().writer();
    for (columnIndexs, 0..) |col_index, iter| {
        if (col_index < row.fields.len) {
            if (iter > 0) {
                try stdOutWriter.print("|", .{});
            }
            if (col_index == 0) {
                try stdOutWriter.print("{d}", .{row.rowId});
            } else {
                try stdOutWriter.print("{s}", .{row.fields[col_index]});
            }
        } else {
            try stdOutWriter.print("NULL ", .{});
        }
    }
    try stdOutWriter.print("\n", .{});
}

fn find_table_root_page(file: std.fs.File, table_name: []const u8) !struct { root_page: u32, sql: []const u8 } {
    const filePtr = @constCast(&file);
    var parser = Parser.init(filePtr);
    const cellPointers = try parser.parse_cellpointer_array(1);
    defer cellPointers.deinit();
    const tableRootPages =
        try readRootPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);
    const table_names = tableRootPages.tables;
    const root_pages = tableRootPages.rootPages;
    const sqls = tableRootPages.sqls;
    var index: ?usize = null;
    for (table_names.items, 0..) |item, i| {
        if (std.mem.eql(u8, item, table_name)) {
            index = i;
            break;
        }
    }
    if (index == null) return error.TableNotFound;
    return .{ .root_page = root_pages.items[index.?], .sql = sqls.items[index.?] };
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
