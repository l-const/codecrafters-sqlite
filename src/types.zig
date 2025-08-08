const std = @import("std");
const ROOT_CELL_SIZE_OFFSET: u16 = @import("./globals.zig").ROOT_CELL_SIZE_OFFSET;
const SQLITE_DEFAULT_PAGE_SIZE = @import("./globals.zig").SQLITE_DEFAULT_PAGE_SIZE;
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
