const std = @import("std");

pub const TokenType = enum {
    Identifier,
    Keyword,
    Number,
    String,
    Symbol,
    Whitespace,
    Comment,
    EOF,
};

pub const Token = struct {
    typ: TokenType,
    value: []const u8,
};

pub const Lexer = struct {
    input: []u8,
    pos: usize,

    pub fn init(input: []const u8, allocator: std.mem.Allocator) !Lexer {
        const owned_input = try allocator.alloc(u8, input.len);
        @memcpy(owned_input, input);
        return Lexer{ .input = owned_input, .pos = 0 };
    }

    pub fn deinit(self: *Lexer, allocator: std.mem.Allocator) void {
        allocator.free(self.input);
    }

    fn peek(self: *Lexer) ?u8 {
        if (self.pos >= self.input.len) return null;
        return self.input[self.pos];
    }

    pub fn reset(self: *Lexer, input: []const u8, allocator: std.mem.Allocator) !void {
        self.pos = 0;
        const owned_input = try allocator.alloc(u8, input.len);
        @memcpy(owned_input, input);
        self.deinit(allocator);
        self.input = owned_input;
    }

    fn advance(self: *Lexer) void {
        self.pos += 1;
    }

    pub fn nextToken(self: *Lexer) !Token {
        while (self.peek()) |c| {
            if (std.ascii.isWhitespace(c)) {
                self.advance();
                continue;
            }
            if (c == '-') {
                if (self.pos + 1 < self.input.len and self.input[self.pos + 1] == '-') {
                    // Single-line comment
                    self.advance();
                    self.advance();
                    while (self.peek()) |cc| {
                        if (cc == '\n') break;
                        self.advance();
                    }
                    continue;
                }
            }
            if (c == '/' and self.pos + 1 < self.input.len and self.input[self.pos + 1] == '*') {
                // Multi-line comment
                self.advance();
                self.advance();
                while (self.peek()) |cc| {
                    if (cc == '*' and self.pos + 1 < self.input.len and self.input[self.pos + 1] == '/') {
                        self.advance();
                        self.advance();
                        break;
                    }
                    self.advance();
                }
                continue;
            }
            if (std.ascii.isAlphabetic(c) or c == '_') {
                const start = self.pos;
                while (self.peek()) |cc| {
                    if (!(std.ascii.isAlphanumeric(cc) or cc == '_')) break;
                    self.advance();
                }
                const value = self.input[start..self.pos];
                // SQLite keywords (partial)
                const keywords = [_][]const u8{ "SELECT", "COUNT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "TABLE", "INDEX", "INTO", "VALUES", "AND", "OR", "NOT", "NULL", "PRIMARY", "KEY", "INTEGER", "TEXT", "REAL", "BLOB", "AUTOINCREMENT", "IF", "NOT", "EXISTS" };
                for (keywords) |kw| {
                    if (std.ascii.eqlIgnoreCase(value, kw)) {
                        return Token{ .typ = TokenType.Keyword, .value = value };
                    }
                }
                return Token{ .typ = TokenType.Identifier, .value = value };
            }
            if (std.ascii.isDigit(c)) {
                const start = self.pos;
                while (self.peek()) |cc| {
                    if (!std.ascii.isDigit(cc)) break;
                    self.advance();
                }
                return Token{ .typ = TokenType.Number, .value = self.input[start..self.pos] };
            }
            if (c == '\'' or c == '"') {
                const quote = c;
                self.advance();
                const start = self.pos;
                while (self.peek()) |cc| {
                    if (cc == quote) break;
                    self.advance();
                }
                const value = self.input[start..self.pos];
                self.advance(); // skip closing quote
                return Token{ .typ = TokenType.String, .value = value };
            }
            // Symbols
            const symbols = [_]u8{ '(', ')', ',', ';', '*', '=', '<', '>', '.', '+', '-', '/' };
            for (symbols) |sym| {
                if (c == sym) {
                    self.advance();
                    return Token{ .typ = TokenType.Symbol, .value = self.input[self.pos - 1 .. self.pos] };
                }
            }
            // Unknown character, skip
            self.advance();
        }
        return Token{ .typ = TokenType.EOF, .value = "" };
    }
};

test "basic SELECT NAME from APPLES" {
    const input = "SELECT NAME FROM APPLES";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);
    var token = try lexer.nextToken();
    std.debug.print("\nToken: {s}\n", .{token.value});
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "SELECT"));
    token = try lexer.nextToken();
    std.debug.print("Token: {s}\n", .{token.value});
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "NAME"));
    token = try lexer.nextToken();
    std.debug.print("Token: {s}\n", .{token.value});
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "FROM"));
    token = try lexer.nextToken();
    std.debug.print("Token: {s}\n", .{token.value});
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "APPLES"));
}

test "SELECT with reset" {
    const input = "SELECT NAME FROM APPLES";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);

    // Reset the lexer with a new input
    const new_input = "SELECT color FROM FRUITS";
    try lexer.reset(new_input, allocator);

    var token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "SELECT"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "color"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "FROM"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "FRUITS"));
}

test "tokenize_count_star - SELECT COUNT(*) FROM APPLES" {
    const input = "SELECT COUNT(*) FROM APPLES";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);

    var token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "SELECT"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "COUNT"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "("));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "*"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ")"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "FROM"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "APPLES"));
}

test "tokenize select with a WHERE clause" {
    const input = "SELECT * FROM APPLES WHERE color = 'Yellow'";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);

    var token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "SELECT"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "*"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "FROM"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "APPLES"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "WHERE"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "color"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "="));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.String);
    try std.testing.expect(std.mem.eql(u8, token.value, "Yellow"));
}

test "basic CREATE TABLE with symbol checks" {
    const input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);

    var token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "CREATE"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "TABLE"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "test"));

    // Check for left parenthesis
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "("));

    // Check for id identifier
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "id"));

    // Check for INTEGER keyword
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "INTEGER"));

    // Check for PRIMARY keyword
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "PRIMARY"));

    // Check for KEY keyword
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "KEY"));

    // Check for comma symbol
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    // Check for name identifier
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "name"));

    // Check for TEXT keyword
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "TEXT"));

    // Check for right parenthesis
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ")"));

    // Should be EOF now
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.EOF);
}

test "CREATE TABLE IF NOT EXISTS superheroes" {
    const input = "CREATE TABLE IF NOT EXISTS \"superheroes\" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text)";
    const allocator = std.testing.allocator;
    var lexer = try Lexer.init(input, allocator);
    defer lexer.deinit(allocator);

    var token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "CREATE"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "TABLE"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "IF"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "NOT"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "EXISTS"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.String);
    try std.testing.expect(std.mem.eql(u8, token.value, "superheroes"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, "("));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "id"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "integer"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "primary"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "key"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "autoincrement"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "name"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "text"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "not"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "null"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "eye_color"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "text"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "hair_color"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "text"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "appearance_count"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "integer"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "first_appearance"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "text"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ","));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Identifier);
    try std.testing.expect(std.mem.eql(u8, token.value, "first_appearance_year"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Keyword);
    try std.testing.expect(std.mem.eql(u8, token.value, "text"));

    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.Symbol);
    try std.testing.expect(std.mem.eql(u8, token.value, ")"));

    // Should be EOF now
    token = try lexer.nextToken();
    try std.testing.expectEqual(token.typ, TokenType.EOF);
}
