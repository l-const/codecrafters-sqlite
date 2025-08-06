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
    var stdOutWriter = std.io.getStdOut().writer();
    try stdOutWriter.print("Listing tables is not implemented yet.\n", .{});
    const filePtr = @constCast(&file);
    var parser = Parser.init("CREATE TABLE", filePtr);
    const cellPointers = try parser.parse_cellpointer_array();
    defer cellPointers.deinit();
    std.debug.print("Cell pointers: size: {d}, page: {d} , items: {}\n", .{ cellPointers.size, cellPointers.pageNo, try cellPointers.get_nth_offset(1) });
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

    pub fn get_cells_pointers(self: *Self) []u16 {
        return self.cells_pointers;
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
