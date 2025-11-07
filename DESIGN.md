# append-fs Design Specification

## 1. Overview
append-fs is a user-space filesystem implemented with the libfuse 3 high-level API. It is intended to serve as the writable upper layer of an overlay filesystem. The implementation emphasizes append-only persistence for both data and metadata, minimal external dependencies, and predictable write behavior suitable for crash recovery via log replay.

Key properties:
- Data and metadata are persisted in separate append-only files inside a backing directory (`$dir/data` and `$dir/meta`).
- Metadata follows a Bitcask-inspired record log that can be replayed on startup to reconstruct the entire filesystem state.
- File data is written in contiguous chunks to `$dir/data`. Per-handle buffering coalesces writes to the optimal 4 MiB chunk size and the minimum 4 KiB granularity.
- Eventual durability is acceptable for regular operations, while `fsync`/`fsyncdir` enforce on-demand persistence.
- Hard links are not supported in the initial version; the `link()` operation will return `EOPNOTSUPP`.

The implementation language is ISO C11. libfuse already provides a mature C API, enabling close control over memory and I/O while keeping the dependency surface small. Alternative languages (e.g., Rust, Go) could offer additional safety features, but C aligns best with libfuse’s design expectations and simplifies deployment on minimal systems. Future versions can consider language bindings if stronger guarantees or faster iteration are required.

## 2. Storage Layout
```
$dir/
  data        # append-only data segments for file contents
  meta        # append-only metadata log
  lock        # optional advisory lockfile to ensure single-writer safety
```
The filesystem process opens both `data` and `meta` with `O_APPEND | O_SYNC` to guarantee ordered appends. The `lock` file is acquired with `flock()` to prevent concurrent mount instances.

## 3. On-Disk Metadata Format
### 3.1 Record Envelope
Each metadata entry is stored as:
```
struct meta_record_header {
    uint8_t  type;       // record_type enum
    uint8_t  version;    // currently 0
    uint16_t reserved;
    uint32_t length;     // payload length in bytes
    uint32_t checksum;   // CRC32C of header (type..length) and payload
};
uint8_t payload[length];
```
Records are little-endian. The checksum allows ignoring partially written or corrupted records during replay.

### 3.2 Record Types
| Type | Purpose |
| ---- | ------- |
| `INODE_CREATE` | Introduce a new inode with initial mode, uid, gid, timestamps, and file type. |
| `INODE_UPDATE` | Update mutable inode fields (size, timestamps, link count, permissions). |
| `DIR_ENTRY_SET` | Associate a name with an inode inside a directory. Used for create, mkdir, rename target, and symlink. |
| `DIR_ENTRY_REMOVE` | Remove a name from a directory. Used for unlink, rmdir, rename source. |
| `EXTENT_APPEND` | Register a new data extent for an inode (offset, length, data file position). |
| `EXTENT_TRUNCATE` | Discard extents beyond a new file size. |
| `SYMLINK_SET` | Store symlink target string for an inode. |
| `XATTR_SET` | Create or update an extended attribute (name, value). |
| `XATTR_REMOVE` | Remove an extended attribute. |
| `INODE_DELETE` | Mark inode as deleted once the last directory reference is gone. |

Because hard links are unsupported, link counts only ever reach 1 for regular files and directories. `INODE_DELETE` is emitted when the sole directory entry is removed.

### 3.3 Combined Rename Records
Renames are serialized as an atomic pair of `DIR_ENTRY_REMOVE` (source) and `DIR_ENTRY_SET` (destination) written back-to-back under a single logical operation lock. Replay ensures both records are applied if present; if the log ends mid-operation, the checksum failure on the second record causes the rename to be treated as incomplete, and the filesystem falls back to only the applied subset consistent with POSIX crash semantics.

### 3.4 Log Replay
On mount:
1. Open `$dir/meta` and sequentially read records.
2. Verify each checksum; stop at the first failure or EOF.
3. Apply records to in-memory structures described in §4.
4. Open `$dir/data` with `lseek(fd, 0, SEEK_END)` to track the next append position.

Since compaction is out of scope, the log grows without bound. Operators can truncate the filesystem by remounting on a fresh directory if necessary.

## 4. In-Memory Data Structures
- **Inode Table**: Hash map keyed by inode number containing metadata (mode, uid, gid, size, timestamps, symlink target, xattrs).
- **Directory Index**: Hash map from `(parent_inode, name)` to child inode. Each directory inode maintains an ordered vector of its entries for `readdir` stability.
- **Extent Table**: For each regular file inode, an ordered list of `struct extent { uint64_t file_offset; uint32_t length; uint64_t data_offset; }` covering the file.
- **Open Handle Table**: Map from `fuse_file_info->fh` to per-handle state (current offset, pending write buffer, dirty flag, open flags).

Each inode structure carries a `pthread_mutex_t` to coordinate concurrent reads and writes. Directory modifications take the parent’s lock. Global structures (inode map) use a read-write lock to allow concurrent lookups.

## 5. Write Buffering & Data File Management
### 5.1 Buffering Strategy
- Every write-capable file handle owns a heap-allocated buffer sized up to 4 MiB.
- Incoming writes are copied into the buffer; once the buffer reaches 4 MiB or the handle is flushed/closed, the buffer is appended to `$dir/data` in a single `write()`.
- Writes smaller than 4 KiB remain buffered until the buffer accumulates at least 4 KiB or an explicit flush occurs.

### 5.2 Flush Triggers
- `write_buf`: flush when buffer size ≥ 4 MiB.
- `flush`, `release`, `fsync`, `fsyncdir`, `truncate`, `lseek` with `SEEK_SET`/`SEEK_CUR` when the new position would leave a gap, and `FUSE_FDATASYNC` flag.
- Periodic timer (e.g., 5 seconds) to mitigate data loss; implemented with a background thread scanning open handles.

### 5.3 Extent Recording
Whenever buffered data is written to `$dir/data`, the filesystem immediately:
1. Issues `pwrite()` to append the buffer to `$dir/data`.
2. On success, appends an `EXTENT_APPEND` record with the file offset, length, and data offset.
3. Updates the in-memory extent list and inode size.
4. Appends an `INODE_UPDATE` record reflecting the new size and timestamps.

Writes that partially overwrite existing regions append new extents; reads pick the newest extent covering a given offset. To avoid holes, `write_buf` flushes outstanding buffers before servicing non-sequential writes.

## 6. Read Path
Reads consult the inode’s extent list to locate the newest extent covering each requested range. The implementation maintains extents sorted by `file_offset` and deduplicates overlapping regions by preserving only the most recent entry per byte during replay and update. Data is retrieved with `pread()` on `$dir/data`, copying the requested slice into FUSE’s response buffer.

Symlink targets are stored entirely in metadata (`SYMLINK_SET`) and read without touching `$dir/data`.

## 7. FUSE Operation Semantics
### 7.1 Implemented Operations
| Operation | Behavior |
| --------- | -------- |
| `getattr` | Populate attributes from inode table. |
| `access` | Check permissions using inode mode, uid, gid, and caller credentials. |
| `opendir` / `readdir` / `releasedir` | Enumerate directory entries cached in memory. |
| `mkdir` | Allocate inode, insert directory entry, emit metadata records, create `.` and `..`. |
| `create` | Allocate inode and directory entry, open handle with fresh buffer. |
| `unlink` | Remove directory entry, emit `DIR_ENTRY_REMOVE`, mark inode deleted (and `INODE_DELETE`). |
| `rmdir` | As `unlink`, after verifying directory is empty. |
| `open` | Initialize handle state; read-only opens skip buffer allocation. |
| `read` | Serve from extent list. |
| `write_buf` | Buffer data and flush per policy. |
| `statfs` | Proxy `statvfs($dir)` and subtract the sizes of `$dir/data`/`meta`. |
| `flush` | Flush handle buffer and append pending metadata. |
| `release` | Flush if dirty, free buffer, and drop handle reference. |
| `lseek` | Support `SEEK_SET`, `SEEK_CUR`, `SEEK_END`; `SEEK_DATA/HOLE` return nearest extent boundary. |
| `readlink` | Return symlink target stored in metadata. |
| `symlink` | Allocate inode with symlink type, store target with `SYMLINK_SET`. |
| `rename` | Emit back-to-back `DIR_ENTRY_REMOVE` + `DIR_ENTRY_SET` records under locks. |
| `utime` / `utimensat` | Update inode timestamps (`INODE_UPDATE`). |
| `truncate` | Flush buffers, adjust size, append `EXTENT_TRUNCATE` and `INODE_UPDATE`. |
| `fsync` | Flush handle buffer, `fdatasync(data_fd)`, `fdatasync(meta_fd)`. |
| `setxattr` / `getxattr` / `listxattr` / `removexattr` | Manage xattrs via metadata records. |
| `fsyncdir` | Flush all open handles within the directory, then `fdatasync(meta_fd)`.

### 7.2 Unsupported Operation
`link` returns `-EOPNOTSUPP`. Callers relying on hard links must fail fast.

## 8. Durability Guarantees
- Regular operations rely on eventual flushing. Buffered data is persisted when natural triggers occur or when the background flusher runs.
- `fsync` and `fsyncdir` guarantee durability by synchronously flushing buffers and issuing `fdatasync` on both data and metadata files before returning success.
- Crash recovery replays all fully written records; incomplete trailing records are ignored due to checksum mismatch.

## 9. Concurrency & Synchronization
- Global read-write lock guards the inode/directory maps during lookups and mutations.
- Per-inode mutex serializes updates to extents, timestamps, and buffers.
- Rename acquires both parent directory locks (ordered by inode number to avoid deadlocks).
- Background flush thread coordinates through condition variables signaled when buffers become flushable.

## 10. Error Handling
- Disk-full (`ENOSPC`) and I/O errors propagate immediately to the FUSE request.
- If flushing a buffer fails after data was appended but before metadata recorded, the filesystem rolls back in-memory extent changes and truncates `$dir/data` using `ftruncate` to the previous offset.
- Log replay stops at the first checksum failure and emits a warning; the filesystem mounts using the state derived from valid records.

## 11. Testing Plan
1. **Unit Tests (hosted userspace):**
   - Metadata parser/serializer round-trips.
   - Extent list maintenance and conflict resolution.
   - Buffer flush logic under sequential and random writes.
2. **Integration Tests (FUSE mount):**
   - Basic POSIX workflows (create/write/read/unlink/mkdir/rmdir/rename).
   - fsync semantics by injecting crashes (kill -9) and verifying recovery.
   - Extended attribute operations.
3. **Stress Tests:**
   - Parallel writers/readers to validate locking.
   - Large file writes to confirm sustained 4 MiB chunks.

## 12. Future Enhancements
- Metadata and data compaction to reclaim space.
- Support for hard links by introducing reference-counted inodes.
- Memory pressure handling for write buffers (e.g., shared buffer pool or spill-to-disk).
- Integrity snapshots or checkpoints for faster mount.

