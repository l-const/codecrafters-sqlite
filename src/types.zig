const std = @import("std");
const readVarIntFromSlice = @import("./varint.zig").readVarIntFromSlice;
const varint_byte_count = @import("./varint.zig").varint_byte_count;
const serialTypeToContentSize = @import("./utils.zig").serialTypeToContentSize;

const allocator = @import("./globals.zig").allocator;
const ROOT_CELL_SIZE_OFFSET: u16 = @import("./globals.zig").ROOT_CELL_SIZE_OFFSET;
const SQLITE_DEFAULT_PAGE_SIZE = @import("./globals.zig").SQLITE_DEFAULT_PAGE_SIZE;
const PAGE_HEADER_TABLE_LEAF_SIZE = @import("./globals.zig").PAGE_HEADER_TABLE_LEAF_SIZE;
const CELL_PTR_SIZE_BYTES = 2; // Size of a cell pointer in bytes
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
        const page_size = readBigEndian(u16, slice[16..18]);
        const write_version = slice[18];
        const read_version = slice[19];
        const reserved_space = slice[20];
        const max_payload_fraction = slice[21];
        const min_payload_fraction = slice[22];
        const leaf_payload_fraction = slice[23];
        const file_change_counter = readBigEndian(u32, slice[24..28]);
        const database_size = readBigEndian(u32, slice[28..32]);
        const first_freelist_page = readBigEndian(u32, slice[32..36]);
        const total_freelist_pages = readBigEndian(u32, slice[36..40]);
        const schema_cookie = readBigEndian(u32, slice[40..44]);
        const schema_format_number = readBigEndian(u32, slice[44..48]);
        const default_page_cache_size = readBigEndian(u32, slice[48..52]);
        const largest_root_page = readBigEndian(u32, slice[52..56]);
        const text_encoding = readBigEndian(u32, slice[56..60]);
        const user_version = readBigEndian(u32, slice[60..64]);
        const incremental_vacuum_mode = readBigEndian(u32, slice[64..68]);
        const application_id = readBigEndian(u32, slice[68..72]);
        const _padding: [20]u8 = slice[72..92].*;
        const version_valid_for = readBigEndian(u32, slice[92..96]);
        const sqlite_version_number = readBigEndian(u32, slice[96..100]);
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
/// Usage: readBigEndian(u32, slice[0..4])
pub fn readBigEndian(comptime T: type, slice: []const u8) T {
    var result: T = 0;
    for (slice) |b| {
        result = (result << 8) | @as(T, b);
    }
    return result;
}

test "readBigEndian" {
    const slice: [4]u8 = [_]u8{ 0x12, 0x34, 0x56, 0x78 };
    const value: u32 = readBigEndian(u32, slice[0..4]);
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

pub const Buffer = struct {
    data: []u8,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(data: []const u8, alloc: std.mem.Allocator) !Buffer {
        const owned_data = try alloc.alloc(u8, data.len);
        @memcpy(owned_data, data);
        return Buffer{ .data = owned_data, .allocator = alloc };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.data);
    }

    pub fn asSlice(self: *const Self) []const u8 {
        return self.data;
    }
};

pub const Row = struct {
    rowId: u64, // Row ID
    payloadSize: u64, // Size of the payload
    payload: []const u8, // The payload data , contains header size , headers and body
    headerSizes: []u64, // Sizes of the headers for each column
    record_body: []const u8, // The body of the record as a whole not separated into fields
    fields: [][]const u8, // The fields of the record
};

pub const PageContent = struct {
    offset: usize, // 100 or 0 depending on the page type (root page containing the DataBaseHeader)
    // The buffer contains the page content, which is a slice of bytes
    buffer: ?Buffer,

    const Self = @This();

    pub fn init(offset: usize, buffer: Buffer) !Self {
        return Self{ .offset = offset, .buffer = buffer };
    }

    pub fn deinit(self: *Self) void {
        self.offset = 0; // Reset offset
        self.buffer = null;
    }

    pub fn asSlice(self: *const Self) []const u8 {
        return self.buffer.?.asSlice();
    }

    pub fn read_u8(self: *const Self, index: usize) u8 {
        if (index >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return self.asSlice()[self.offset + index];
    }

    pub fn read_u16(self: *const Self, index: usize) u16 {
        if (index + 1 >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return readBigEndian(u16, self.asSlice()[self.offset + index .. self.offset + index + 2]);
    }

    pub fn read_u32(self: *const Self, index: usize) u32 {
        if (index + 3 >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return readBigEndian(u32, self.asSlice()[self.offset + index .. self.offset + index + 4]);
    }

    // Methods that ignore self.offset and read directly from the buffer
    pub fn read_u8_at(self: *const Self, index: usize) u8 {
        if (index >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return self.asSlice()[index];
    }

    pub fn read_u16_at(self: *const Self, index: usize) u16 {
        if (index + 1 >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return readBigEndian(u16, self.asSlice()[index .. index + 2]);
    }

    pub fn read_u32_at(self: *const Self, index: usize) u32 {
        if (index + 3 >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        return readBigEndian(u32, self.asSlice()[index .. index + 4]);
    }

    pub fn read_varint_at(self: *const Self, index: usize) u64 {
        std.debug.print("index: {d}, slice: {any}\n", .{ index, self.asSlice().len });
        std.debug.assert(self.buffer.?.asSlice().len > index);
        if (index >= self.asSlice().len) {
            @panic("Index out of bounds");
        }
        const result = readVarIntFromSlice(self.asSlice()[index..]) catch 0;
        return result;
    }

    pub fn getPageType(self: *const Self) PageType {
        return @enumFromInt(self.asSlice()[self.offset + 0]);
    }

    pub fn cell_count(self: *const Self) u16 {
        return self.read_u16(3);
    }

    pub fn cell_pointer_array_size(self: *const Self) usize {
        // The size of the cell pointer array is the number of cells times the size of a cell pointer
        return self.cell_count() * CELL_PTR_SIZE_BYTES;
    }

    pub fn getCellPointerArray(self: *const Self) ![]u16 {
        const cell_counter = self.cell_count();
        var cell_pointers = try std.ArrayList(u16).initCapacity(allocator, cell_counter);
        defer cell_pointers.deinit();

        // Read the cell pointers from the page content
        for (0..cell_counter) |i| {
            const offset = self.read_u16(PAGE_HEADER_TABLE_LEAF_SIZE + i * CELL_PTR_SIZE_BYTES);
            std.debug.print("Cell pointer {d}: {d}\n", .{ i, offset });
            _ = try cell_pointers.append(offset);
        }

        return cell_pointers.toOwnedSlice();
    }

    pub fn getRows(self: *const Self) ![]Row {
        const pointerArray = try self.getCellPointerArray();
        // defer pointerArray.deinit();
        // Here you can process the rows as needed
        var rows = try std.ArrayList(Row).initCapacity(allocator, pointerArray.len);
        defer rows.deinit();
        for (0..pointerArray.len) |i| {
            const rowOffset = pointerArray[i];
            std.debug.print("Processing row at offset {d}\n", .{rowOffset});
            // Process each row
            std.debug.print("Row {d}: {d}\n", .{ i, rowOffset });
            const payloadSize = self.read_varint_at(rowOffset);
            std.debug.print("Payload size for row {d}: {d}\n", .{ i, payloadSize });
            const rowIdStart = rowOffset + varint_byte_count(payloadSize);
            const rowId = self.read_varint_at(rowIdStart);
            std.debug.print("Row ID for row {d}: {d}\n", .{ i, rowId });
            const recordHeaderSizeStart = rowIdStart + varint_byte_count(rowId);
            std.debug.print("Record header size start for row {d}: {d}\n", .{ i, recordHeaderSizeStart });
            const recordHeaderSize = self.read_varint_at(recordHeaderSizeStart);
            std.debug.print("Record header size for row {d}: {d}\n", .{ i, recordHeaderSize });
            const recordHeaderStart = recordHeaderSizeStart + varint_byte_count(recordHeaderSize);
            std.debug.print("Record header start for row {d}: {d}\n", .{ i, recordHeaderStart });
            var headerSizes = try std.ArrayList(u64).initCapacity(allocator, 1);
            var bytes_read: u16 = 0;
            var offsetStart = recordHeaderStart;
            while (bytes_read < recordHeaderSize) {
                const headerSize = self.read_varint_at(offsetStart);
                std.debug.print("Header size for row {d}: {d}\n", .{ i, headerSize });
                _ = try headerSizes.append(serialTypeToContentSize(headerSize));
                bytes_read += varint_byte_count(headerSize);
                offsetStart += 1;
            }
            const payloadBytesRead = recordHeaderSize;
            const recordBodyStart = recordHeaderStart + payloadBytesRead - 1;
            var bytes_of_body_read: u64 = 0;
            for (0..headerSizes.items.len - 1) |k| {
                std.debug.print("Header size {d}: {d}\n", .{ k, headerSizes.items[k] });
                bytes_of_body_read += headerSizes.items[k];
            }
            const body_row_size = bytes_of_body_read;
            var fields = try std.ArrayList([]const u8).initCapacity(allocator, 1);
            var j: u16 = 0;
            var offset = recordBodyStart;
            bytes_of_body_read = 0;
            while (bytes_of_body_read < body_row_size) {
                std.debug.print("Reading field {d} at offset {d} headerSize: {d}\n", .{ j, offset, headerSizes.items[j] });
                const field = self.asSlice()[offset .. offset + headerSizes.items[j]];
                bytes_of_body_read += field.len;
                try fields.append(field);
                offset += headerSizes.items[j];
                std.debug.print("Field {d}: , offset: {d}\n", .{ j, offset });

                j += 1;
            }
            const row = Row{
                .rowId = rowId,
                .payloadSize = payloadSize,
                .payload = self.asSlice()[recordHeaderSizeStart .. recordHeaderSizeStart + payloadSize],
                .headerSizes = headerSizes.items,
                .record_body = self.asSlice()[recordBodyStart .. recordBodyStart + body_row_size],
                .fields = fields.items,
            };
            _ = try rows.append(row);
            std.debug.print("Payload size for row {d}: {d}\n", .{ i, payloadSize });
        }
        return rows.items;
    }
};

test "PageContent getPageType + cell_count" {
    const buffer_data = [_]u8{ 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00, 0x10, 0x00, 0x01, 0x01, 0x00, 0x40, 0x20, 0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x2e, 0x7e, 0x5a, 0x0d, 0x00, 0x00, 0x00, 0x03, 0x0e, 0xc3, 0x00, 0x0f, 0x8f, 0x0f, 0x3d, 0x0d, 0x00, 0x00, 0x00, 0x03 };
    var buffer = try Buffer.init(&buffer_data, std.testing.allocator);
    var page_content = try PageContent.init(100, buffer);
    defer page_content.deinit();
    defer buffer.deinit();

    const page_type = page_content.getPageType();
    try std.testing.expectEqual(PageType.TableLeaf, page_type);
    const cell_count = page_content.cell_count();
    try std.testing.expectEqual(3, cell_count);
    const cell_pointer_array_size = page_content.cell_pointer_array_size();
    try std.testing.expectEqual(6, cell_pointer_array_size); // 3 cells * CELL_PTR_SIZE_BYTES (2 bytes each)
}
