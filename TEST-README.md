
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