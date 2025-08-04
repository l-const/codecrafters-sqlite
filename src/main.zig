const std = @import("std");

const SQLITE_DEFAULT_PAGE_SIZE: u16 = 4096; // Default page size for SQLite
const SQLITE_HEADER_SIZE: u16 = 100; // Size of the SQLite header
const ROOT_CELL_SIZE_OFFSET: u16 = 3; // Offset for the root cell size in the header

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
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        // You can use print statements as follows for debugging, they'll be visible when running tests.
        std.debug.print("Logs from your program will appear here!\n", .{});

        // Uncomment this block to pass the first stage
        var buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&buf);
        const page_size = std.mem.readInt(u16, &buf, .big);
        var stdOutWriter = std.io.getStdOut().writer();
        try stdOutWriter.print("database page size: {}\n", .{page_size});
        var parser = RegexFileParser.init("CREATE TABLE", &file);
        // try parser.parse();
        const noOfTables = try parser.get_tables_count();
        try stdOutWriter.print("number of tables: {d}\n", .{noOfTables});
    }
}

const RegexFileParser = struct {
    // Define your parser structure here
    // This is a placeholder for the actual implementation
    count: u32,
    regex: []const u8,
    file: *std.fs.File,

    const Self = @This();

    pub fn init(regex: []const u8, file: *std.fs.File) Self {
        return Self{ .count = 0, .regex = regex, .file = file };
    }

    pub fn occurrences(self: *Self) u32 {
        return self.count;
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
