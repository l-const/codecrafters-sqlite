const std = @import("std");
const Lexer = @import("./lexer.zig").Lexer;
const TokenType = @import("./lexer.zig").TokenType;
const Token = @import("./lexer.zig").Token;

pub const StatementType = enum {
    Select,
    CreateTable,
    Unknown,
};

pub const SQLiteColumnType = enum {
    Integer,
    Text,
    Real,
    Blob,
    Null,
    Unknown,
};

pub const Column = struct {
    name: []const u8,
    typ: SQLiteColumnType,
};

pub const WhereClause = struct {
    column: []const u8,
    operator: Operator,
    value: []const u8,
};

pub const Operator = enum {
    Equal,
    NotEqual,
};

pub const SelectStatement = struct {
    columns: std.ArrayList([]const u8),
    is_count: bool = false, // Indicates if the SELECT statement is an aggregate query
    // functions: std.ArrayList([]const u8), // List of functions applied to columns, e.g., COUNT, SUM
    where_clause: ?WhereClause = null, // Optional WHERE clause
    // functions_to_columns: std.AutoHashMap([]const u8, std.ArrayList([]const u8)),
    table: []const u8,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.columns.deinit();
        // self.functions.deinit();
        // self.functions_to_columns.deinit();
    }
};

pub const CreateTableStatement = struct {
    table: []const u8,
    columns: std.ArrayList(Column), // ArrayList of column definitions

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.columns.deinit();
    }
};

pub const Statement = union(StatementType) {
    Select: SelectStatement,
    CreateTable: CreateTableStatement,
    Unknown: void,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        switch (self.*) {
            .Select => self.Select.deinit(),
            .CreateTable => self.CreateTable.deinit(),
            .Unknown => {},
        }
    }
};

pub const SQLParser = struct {
    lexer: Lexer,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(input: []const u8, allocator: std.mem.Allocator) !SQLParser {
        const lexer = try Lexer.init(input, allocator);
        return SQLParser{ .lexer = lexer, .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.lexer.deinit(self.allocator);
    }

    pub fn parse(self: *Self, input: ?[]const u8) !Statement {
        if (input) |inp| {
            try self.lexer.reset(inp, self.allocator);
        }
        const token = try self.lexer.nextToken();
        if (token.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(token.value, "SELECT")) {
            return try self.parse_select();
        } else if (token.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(token.value, "CREATE")) {
            return try self.parse_create_table();
        }
        return Statement{ .Unknown = {} };
    }

    fn parse_select(self: *Self) !Statement {
        var columns = std.ArrayList([]const u8).init(self.allocator);
        var is_count = false;
        var next = try self.lexer.nextToken();

        // SPECIAL CASE: Handle COUNT(*) syntax
        if (next.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(next.value, "COUNT")) {
            is_count = true;
            next = try self.lexer.nextToken(); // LEFT PAREN
            std.debug.assert(next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, "("));
            next = try self.lexer.nextToken(); // Column name or *
            if (next.typ == TokenType.Identifier or next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, "*")) {
                try columns.append(next.value);
                next = try self.lexer.nextToken(); // RIGHT PAREN
                std.debug.assert(next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, ")"));
                // return Statement{ .Select = SelectStatement{ .columns = columns, .is_count = true, .table = "" } };
            } else {
                return Statement{ .Unknown = {} };
            }
        } else {
            // Normal SELECT statement - parse comma-separated columns
            while (next.typ == TokenType.Identifier or next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, ",")) {
                if (next.typ == TokenType.Identifier) {
                    try columns.append(next.value);
                }
                next = try self.lexer.nextToken();
            }
        }

        if (is_count) {
            next = try self.lexer.nextToken();
        }

        var where_clause: ?WhereClause = null;

        if (next.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(next.value, "FROM")) {
            next = try self.lexer.nextToken();
            if (next.typ == TokenType.Identifier) {
                const table = next.value;
                next = try self.lexer.nextToken();

                // Parse WHERE clause if present
                while (next.typ != TokenType.EOF) {
                    if (next.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(next.value, "WHERE")) {
                        next = try self.lexer.nextToken();
                        if (next.typ == TokenType.Identifier) {
                            const column = next.value;
                            next = try self.lexer.nextToken();
                            var op: Operator = Operator.Equal;
                            if (next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, "=")) {
                                op = Operator.Equal;
                            } else if (next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, "!=")) {
                                op = Operator.NotEqual;
                            } else {
                                return Statement{ .Unknown = {} };
                            }
                            next = try self.lexer.nextToken();
                            if (next.typ == TokenType.Identifier or next.typ == TokenType.String) {
                                const value = next.value;
                                where_clause = WhereClause{
                                    .column = column,
                                    .operator = op,
                                    .value = value,
                                };
                            } else {
                                return Statement{ .Unknown = {} };
                            }
                            next = try self.lexer.nextToken();
                        } else {
                            return Statement{ .Unknown = {} };
                        }
                    } else {
                        // Skip other tokens (e.g., ORDER BY, LIMIT, etc.)
                        next = try self.lexer.nextToken();
                    }
                }

                return Statement{ .Select = SelectStatement{
                    .columns = columns,
                    .is_count = is_count,
                    .table = table,
                    .where_clause = where_clause,
                } };
            }
        }
        return Statement{ .Unknown = {} };
    }

    fn parse_column(column_def: []const u8) Column {
        var name: []const u8 = "";
        var typ: SQLiteColumnType = SQLiteColumnType.Unknown;
        var it = std.mem.tokenizeAny(u8, column_def, " ");
        if (it.next()) |col_name| {
            name = col_name;
        }
        if (it.next()) |col_type| {
            if (std.ascii.eqlIgnoreCase(col_type, "INTEGER")) {
                typ = SQLiteColumnType.Integer;
            } else if (std.ascii.eqlIgnoreCase(col_type, "TEXT")) {
                typ = SQLiteColumnType.Text;
            } else if (std.ascii.eqlIgnoreCase(col_type, "REAL")) {
                typ = SQLiteColumnType.Real;
            } else if (std.ascii.eqlIgnoreCase(col_type, "BLOB")) {
                typ = SQLiteColumnType.Blob;
            } else if (std.ascii.eqlIgnoreCase(col_type, "NULL")) {
                typ = SQLiteColumnType.Null;
            } else {
                typ = SQLiteColumnType.Unknown;
            }
        }
        return Column{ .name = name, .typ = typ };
    }

    fn parse_create_table(self: *Self) !Statement {
        // Parse CREATE TABLE <table> (...)
        var next = try self.lexer.nextToken();
        if (next.typ == TokenType.Keyword and std.ascii.eqlIgnoreCase(next.value, "TABLE")) {
            next = try self.lexer.nextToken();
            // IF NOT EXISTS - skip
            while (next.typ != TokenType.EOF and next.typ != TokenType.Identifier and next.typ != TokenType.String) {
                next = try self.lexer.nextToken();
            }
            if (next.typ == TokenType.Identifier or next.typ == TokenType.String) {
                const table = next.value;
                next = try self.lexer.nextToken();
                if (next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, "(")) {
                    var column_defs = std.ArrayList([]const u8).init(self.allocator);
                    defer column_defs.deinit();
                    next = try self.lexer.nextToken();
                    while (!(next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, ")"))) {
                        if (next.typ == TokenType.Identifier) {
                            // Collect column name and type as a string
                            const col_start_pos = self.lexer.pos - next.value.len;
                            var col_end_pos = self.lexer.pos;
                            next = try self.lexer.nextToken();
                            while (next.typ == TokenType.Keyword or next.typ == TokenType.Identifier) {
                                col_end_pos = self.lexer.pos;
                                next = try self.lexer.nextToken();
                            }
                            // Slice the column definition from input
                            const col_slice = self.lexer.input[col_start_pos..col_end_pos];
                            try column_defs.append(col_slice);
                        }
                        if (next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, ",")) {
                            next = try self.lexer.nextToken();
                        } else if (!(next.typ == TokenType.Symbol and std.mem.eql(u8, next.value, ")"))) {
                            next = try self.lexer.nextToken();
                        }
                    }
                    // Convert column_defs to ArrayList(Column)
                    var columns = std.ArrayList(Column).init(self.allocator);
                    for (column_defs.items) |col_def| {
                        try columns.append(parse_column(col_def));
                    }
                    return Statement{ .CreateTable = CreateTableStatement{ .table = table, .columns = columns } };
                }
            }
        }
        return Statement{ .Unknown = {} };
    }
};

test "parse SELECT statement" {
    const input = "SELECT name, age FROM apples";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.Select);
    const select = stmt.Select;
    try std.testing.expect(std.mem.eql(u8, select.columns.items[0], "name"));
    try std.testing.expect(std.mem.eql(u8, select.columns.items[1], "age"));
    try std.testing.expect(std.mem.eql(u8, select.table, "apples"));
}

test "parse SELECT with WHERE clause" {
    const input = "SELECT name FROM apples WHERE color = 'Yellow'";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.Select);
    const select = stmt.Select;
    try std.testing.expect(std.mem.eql(u8, select.columns.items[0], "name"));
    try std.testing.expect(std.mem.eql(u8, select.table, "apples"));
    try std.testing.expect(select.where_clause != null);
    const where = select.where_clause.?;
    try std.testing.expect(std.mem.eql(u8, where.column, "color"));
    try std.testing.expect(where.operator == Operator.Equal);
    try std.testing.expect(std.mem.eql(u8, where.value, "Yellow"));
}

test "parse COUNT(*) SELECT statement" {
    const input = "SELECT COUNT(*) FROM apples";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.Select);
    const select = stmt.Select;
    try std.testing.expect(select.is_count);
    try std.testing.expect(select.columns.items.len == 1);
    try std.testing.expect(std.mem.eql(u8, select.columns.items[0], "*"));
    try std.testing.expect(std.mem.eql(u8, select.table, "apples"));
}

test "parse CREATE TABLE statement" {
    const input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.CreateTable);
    const create = stmt.CreateTable;
    try std.testing.expect(std.mem.eql(u8, create.table, "test"));
    try std.testing.expect(create.columns.items.len == 2);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[0].name, "id"));
    try std.testing.expect(create.columns.items[0].typ == SQLiteColumnType.Integer);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[1].name, "name"));
    try std.testing.expect(create.columns.items[1].typ == SQLiteColumnType.Text);
}

test "parse CREATE TABLE with autoincrement statement" {
    const input = "CREATE TABLE oranges (    id integer primary key autoincrement,   name text, description text )";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.CreateTable);
    const create = stmt.CreateTable;
    try std.testing.expect(std.mem.eql(u8, create.table, "oranges"));
    try std.testing.expect(create.columns.items.len == 3);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[0].name, "id"));
    try std.testing.expect(create.columns.items[0].typ == SQLiteColumnType.Integer);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[1].name, "name"));
    try std.testing.expect(create.columns.items[1].typ == SQLiteColumnType.Text);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[2].name, "description"));
    try std.testing.expect(create.columns.items[2].typ == SQLiteColumnType.Text);
}

test "parse CREATE TABLE IF NOT EXISTS statement" {
    const input = "CREATE TABLE IF NOT EXISTS \"test\" (id INTEGER PRIMARY KEY, name TEXT)";
    const allocator = std.testing.allocator;
    var parser = try SQLParser.init(input, allocator);
    defer parser.deinit();
    var stmt = try parser.parse(null);
    defer stmt.deinit();
    try std.testing.expect(stmt == StatementType.CreateTable);
    const create = stmt.CreateTable;
    try std.testing.expect(std.mem.eql(u8, create.table, "test"));
    try std.testing.expect(create.columns.items.len == 2);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[0].name, "id"));
    try std.testing.expect(create.columns.items[0].typ == SQLiteColumnType.Integer);
    try std.testing.expect(std.mem.eql(u8, create.columns.items[1].name, "name"));
    try std.testing.expect(create.columns.items[1].typ == SQLiteColumnType.Text);
}
