const std = @import("std");
const ROOT_CELL_SIZE_OFFSET: u16 = @import("./globals.zig").ROOT_CELL_SIZE_OFFSET;
const SQLITE_DEFAULT_PAGE_SIZE = @import("./globals.zig").SQLITE_DEFAULT_PAGE_SIZE;

const DataBaseHeader = struct {
    magic: [16]u8, // 16 bytes
    page_size: u16, // 2 bytes
    write_version: u8, // 1 byte
    read_version: u8, // 1 byte
    reserved_space: u8, // 1 byte
    max_payload_fraction: u8, // 1 byte
    min_payload_fraction: u8, // 1 byte
    leaf_payload_fraction: u8, // 1 byte
    file_change_counter: u32, // 4 bytes
    database_size: u32, // 4 bytes
    first_freelist_page: u32, // 4 bytes
    total_freelist_pages: u32, // 4 bytes
    schema_cookie: u32, // 4 bytes
    schema_format_number: u32, // 4 bytes
    default_page_cache_size: u32, // 4 bytes
    largest_root_page: u32, // 4 bytes
    text_encoding: u32, // 4 bytes
    user_version: u32, // 4 bytes
    incremental_vacuum_mode: u32, // 4 bytes
    application_id: u32, // 4 bytes
    _padding: [20]u8, // 20 bytes
    version_valid_for: u32, // 4 bytes
    sqlite_version_number: u32, // 4 bytes

    pub fn default() DataBaseHeader {
        return DataBaseHeader{
            .magic = "SQLite format 3",
            .page_size = SQLITE_DEFAULT_PAGE_SIZE,
            .write_version = 1,
            .read_version = 1,
            .reserved_space = 0,
            .max_payload_fraction = 64,
            .min_payload_fraction = 32,
            .leaf_payload_fraction = 32,
            .database_size = 0, // Initially 0, will be set later
            .file_change_counter = 1,
            .first_freelist_page = 0,
            .total_freelist_pages = 0,
            .schema_cookie = 0,
            .schema_format_number = 4,
            .default_page_cache_size = 0,
            .largest_root_page = 0,
            .text_encoding = 1, // UTF-8
            .user_version = 0,
            .incremental_vacuum_mode = 0,
            .application_id = 0,
            ._padding = [_]u8{0} ** 20, // padding
            .version_valid_for = 3047000, // SQLite version valid for
            .sqlite_version_number = 3047000, // SQLite version number
        };
    }

    pub fn from_slice(input_slice: []const u8) !DataBaseHeader {
        if (input_slice.len != @sizeOf(DataBaseHeader)) {
            return error.InvalidSliceLength;
        }
        // Duplicate the slice to avoid aliasing issues
        var slice: [100]u8 = undefined;
        @memcpy(slice[0..], input_slice[0..100]);
        // Read each field from the slice using comptimeReadBigEndian
        const magic: [16]u8 = slice[0..16].*;
        const page_size = comptimeReadBigEndian(u16, slice[16..18]);
        const write_version = slice[18];
        const read_version = slice[19];
        const reserved_space = slice[20];
        const max_payload_fraction = slice[21];
        const min_payload_fraction = slice[22];
        const leaf_payload_fraction = slice[23];
        const file_change_counter = comptimeReadBigEndian(u32, slice[24..28]);
        const database_size = comptimeReadBigEndian(u32, slice[28..32]);
        const first_freelist_page = comptimeReadBigEndian(u32, slice[32..36]);
        const total_freelist_pages = comptimeReadBigEndian(u32, slice[36..40]);
        const schema_cookie = comptimeReadBigEndian(u32, slice[40..44]);
        const schema_format_number = comptimeReadBigEndian(u32, slice[44..48]);
        const default_page_cache_size = comptimeReadBigEndian(u32, slice[48..52]);
        const largest_root_page = comptimeReadBigEndian(u32, slice[52..56]);
        const text_encoding = comptimeReadBigEndian(u32, slice[56..60]);
        const user_version = comptimeReadBigEndian(u32, slice[60..64]);
        const incremental_vacuum_mode = comptimeReadBigEndian(u32, slice[64..68]);
        const application_id = comptimeReadBigEndian(u32, slice[68..72]);
        const _padding: [20]u8 = slice[72..92].*;
        const version_valid_for = comptimeReadBigEndian(u32, slice[92..96]);
        const sqlite_version_number = comptimeReadBigEndian(u32, slice[96..100]);
        return DataBaseHeader{
            .magic = magic,
            .page_size = page_size,
            .write_version = write_version,
            .read_version = read_version,
            .reserved_space = reserved_space,
            .max_payload_fraction = max_payload_fraction,
            .min_payload_fraction = min_payload_fraction,
            .leaf_payload_fraction = leaf_payload_fraction,
            .file_change_counter = file_change_counter,
            .database_size = database_size,
            .first_freelist_page = first_freelist_page,
            .total_freelist_pages = total_freelist_pages,
            .schema_cookie = schema_cookie,
            .schema_format_number = schema_format_number,
            .default_page_cache_size = default_page_cache_size,
            .largest_root_page = largest_root_page,
            .text_encoding = text_encoding,
            .user_version = user_version,
            .incremental_vacuum_mode = incremental_vacuum_mode,
            .application_id = application_id,
            ._padding = _padding,
            .version_valid_for = version_valid_for,
            .sqlite_version_number = sqlite_version_number,
        };
    }
};

test "DatabaseHeader size" {
    const header_size = @sizeOf(DataBaseHeader);
    try std.testing.expectEqual(@as(usize, 100), header_size); // Adjust the expected size as needed
}

const PageType = enum(u8) {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,

    const Self = @This();

    pub fn is_table(self: *const Self) bool {
        return self.* == Self.TableInterior or self.* == Self.TableLeaf;
    }

    pub fn is_leaf(self: *const Self) bool {
        return self.* == Self.IndexLeaf or self.* == Self.TableLeaf;
    }
};

test "PageType is_table" {
    const interior_index = PageType.IndexInterior;
    const index_leaf = PageType.IndexLeaf;
    const table_interior = PageType.TableInterior;
    const table_leaf = PageType.TableLeaf;

    try std.testing.expect(!interior_index.is_table());
    try std.testing.expect(!index_leaf.is_table());
    try std.testing.expect(table_interior.is_table());
    try std.testing.expect(table_leaf.is_table());
}

test "PageType is_leaf" {
    const interior_index = PageType.IndexInterior;
    const index_leaf = PageType.IndexLeaf;
    const table_interior = PageType.TableInterior;
    const table_leaf = PageType.TableLeaf;

    try std.testing.expect(!interior_index.is_leaf());
    try std.testing.expect(index_leaf.is_leaf());
    try std.testing.expect(!table_interior.is_leaf());
    try std.testing.expect(table_leaf.is_leaf());
}

// Define the CellPointerArray structure
const CellPointerArrayError = error{
    OutOfBounds,
};

pub const CellPointerArray = struct {
    cells_pointers: std.ArrayList(u16), // Using ArrayList for dynamic size
    pageNo: u32 = 0, // page number

    const Self = @This();

    pub fn init(cells_pointers: std.ArrayList(u16)) Self {
        const cellsPtr = @constCast(&cells_pointers);
        const pageNo = Self.current_page_number(cellsPtr);
        return Self{ .cells_pointers = cells_pointers, .pageNo = pageNo };
    }

    pub fn deinit(self: Self) void {
        // Cleanup if necessary
        self.cells_pointers.deinit();
    }

    pub inline fn get_size(self: *const Self) usize {
        return self.cells_pointers.items.len;
    }

    pub inline fn get_page_number(self: *Self) u32 {
        return self.pageNo;
    }

    inline fn current_page_number(cel_pointers: *std.ArrayList(u16)) u32 {
        const offset = cel_pointers.items[0]; // Get the first cell pointer or return 0 if empty
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

/// Reads n bytes from a big-endian slice and returns the value as type T.
/// Usage: comptimeReadBigEndian(u32, slice[0..4])
pub fn comptimeReadBigEndian(comptime T: type, slice: []const u8) T {
    var result: T = 0;
    for (slice) |b| {
        result = (result << 8) | @as(T, b);
    }
    return result;
}

test "comptimeReadBigEndian" {
    const slice: [4]u8 = [_]u8{ 0x12, 0x34, 0x56, 0x78 };
    const value: u32 = comptimeReadBigEndian(u32, slice[0..4]);
    try std.testing.expectEqual(0x12345678, value);
}

test "from_slice parses header from inspect-content.txt" {
    // Hex bytes from inspect-content.txt, first 100 bytes
    const header_bytes: [100]u8 = [_]u8{ 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00, 0x10, 0x00, 0x01, 0x01, 0x00, 0x40, 0x20, 0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x2e, 0x7e, 0x5a, 0x0d, 0x00, 0x00, 0x00, 0x03, 0x0e, 0xc3, 0x00, 0x0f, 0x8f, 0x0f, 0x3d };
    const header = try DataBaseHeader.from_slice(header_bytes[0..]);
    try std.testing.expectEqual(header.magic[0], 'S'); // 'S'
    try std.testing.expectEqual(header.magic[1], 'Q'); // 'Q'
    try std.testing.expectEqual(header.magic[2], 'L'); // 'L'
    try std.testing.expectEqual(header.magic[3], 'i'); // 'i'
    try std.testing.expectEqual(header.page_size, 0x1000); // 4096
    try std.testing.expectEqual(header.write_version, 0x01);
    try std.testing.expectEqual(header.read_version, 0x01);
    // Add more field checks as needed
}
