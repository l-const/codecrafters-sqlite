
#### Test memory leaks

### C allocator
Use malloc - c_allocator
use valgrind

```bash
codecrafters-sqlite-zig on  master:main [!?] via ↯ v0.15.0-dev.905+edf785db0 
❯ valgrind --leak-check=full  --show-leak-kinds=all ./zig-out/bin/main sample.db .tables
```


you want to see 0 butes escaped and 0 errors


```bash
==180089== 
==180089== HEAP SUMMARY:
==180089==     in use at exit: 0 bytes in 0 blocks
==180089==   total heap usage: 18 allocs, 18 frees, 1,180 bytes allocated
==180089== 
==180089== All heap blocks were freed -- no leaks are possible
==180089== 
==180089== For lists of detected and suppressed errors, rerun with: -s
==180089== ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
```


### zig allocator 

Having on fn main a defer statement like:

```zig
defer {
        const leak = gpa.deinit();
        switch (leak) {
            .leak => {
                @panic("Memory leak detected");
            },
            else => {},
        }
    }
```

You will get a panic message and core dumped like so:



```bash
rror(gpa): memory address 0x7f9523280000 leaked: 
/home/kostas/Desktop/codecrafters-sqlite-zig/src/main.zig:164:51: 0x1156140 in readPageRecords (main.zig)
            const schemaName = try allocator.alloc(u8, schemaNameSize);
                                                  ^
/home/kostas/Desktop/codecrafters-sqlite-zig/src/main.zig:82:44: 0x1157858 in tables (main.zig)
    const table_names = try readPageRecords(filePtr, cellPointers.get_cells_pointers(), TableCellType.Leaf);
                                           ^
/home/kostas/Desktop/codecrafters-sqlite-zig/src/main.zig:47:19: 0x1159edb in main (main.zig)
        try tables(file);
                  ^
/usr/local/zig/lib/std/start.zig:681:37: 0x115ae69 in main (std.zig)
            const result = root.main() catch |err| {
                                    ^
???:?:?: 0x7f95232e75f4 in ??? (libc.so.6)
???:?:?: 0x7f95232e76a7 in ??? (libc.so.6)

thread 181823 panic: Memory leak detected
/home/kostas/Desktop/codecrafters-sqlite-zig/src/main.zig:23:17: 0x115a442 in main (main.zig)
                @panic("Memory leak detected");

```

