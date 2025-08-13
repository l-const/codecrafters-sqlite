const std = @import("std");
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
pub const allocator = gpa.allocator();
// const allocator = std.heap.c_allocator;

pub const SQLITE_DEFAULT_PAGE_SIZE: u16 = 4096; // Default page size for SQLite
pub const SQLITE_HEADER_SIZE: u16 = 100; // Size of the SQLite header
pub const ROOT_CELL_SIZE_OFFSET: u16 = 3; // Offset for the root cell size in the header
pub const PAGE_HEADER_TABLE_INTERIOR_SIZE: u16 = 12; // Size of the page header
pub const PAGE_HEADER_TABLE_LEAF_SIZE: u16 = 8; // Size of the page header
pub const SQLITE_SCHEMA_TYPE_INDEX = 1;
pub const SQLITE_SCHEMA_NAME_INDEX = 2;
pub const SQLITE_SCHEMA_TYPE_TABLE_NAME_INDEX = 3;
pub const SQLITE_SCHEMA_TYPE_TABLE_ROOT_PAGE_INDEX = 4; // Index for the table root page in the schema
pub const SQLITE_SCHEMA_TYPE_TABLE_SCHEMA_INDEX = 5; // Index for the table schema in the sqlite_schema
