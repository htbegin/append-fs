#define _GNU_SOURCE
#include "appendfs.h"
#include "crc32.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <utime.h>
#include <time.h>
#include <unistd.h>

#ifndef O_BINARY
#define O_BINARY 0
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#ifndef XATTR_CREATE
#define XATTR_CREATE 0x1
#endif

#ifndef XATTR_REPLACE
#define XATTR_REPLACE 0x2
#endif

#define DATA_FILENAME "data"
#define META_FILENAME "meta"

#define RECORD_HEADER_SIZE 9

enum appendfs_record_type {
    APPENDFS_RECORD_CREATE = 1,
    APPENDFS_RECORD_EXTENT = 2,
    APPENDFS_RECORD_TRUNCATE = 3,
    APPENDFS_RECORD_UNLINK = 4,
    APPENDFS_RECORD_RENAME = 5,
    APPENDFS_RECORD_MKDIR = 6,
    APPENDFS_RECORD_SETXATTR = 7,
    APPENDFS_RECORD_REMOVEXATTR = 8,
    APPENDFS_RECORD_TIMES = 9
};

struct appendfs_extent {
    off_t logical_offset;
    uint32_t length;
    off_t data_offset;
};

struct appendfs_xattr {
    char *name;
    unsigned char *value;
    size_t size;
};

struct appendfs_inode {
    uint64_t inode_id;
    char *path;
    mode_t mode;
    off_t size;
    time_t ctime;
    time_t mtime;
    time_t atime;
    int deleted;
    struct appendfs_extent *extents;
    size_t extent_count;
    size_t extent_capacity;
    char *symlink_target;
    struct appendfs_xattr *xattrs;
    size_t xattr_count;
    size_t xattr_capacity;
};

struct appendfs_context {
    char *root_path;
    int data_fd;
    int meta_fd;
    uint64_t next_inode_id;
    struct appendfs_inode *inodes;
    size_t inode_count;
    size_t inode_capacity;
    size_t write_buffer_size;
};

struct appendfs_file {
    struct appendfs_context *ctx;
    struct appendfs_inode *inode;
    unsigned char *buffer;
    size_t buffer_size;
    size_t buffer_used;
    off_t buffer_offset;
    int flags;
    off_t position;
};

static int ensure_directory(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return 0;
        }
        errno = ENOTDIR;
        return -1;
    }
    if (mkdir(path, 0755) == 0) {
        return 0;
    }
    if (errno == ENOENT) {
        char *tmp = strdup(path);
        if (!tmp) {
            return -1;
        }
        char *slash = strrchr(tmp, '/');
        if (!slash) {
            free(tmp);
            errno = ENOENT;
            return -1;
        }
        *slash = '\0';
        if (ensure_directory(tmp) == -1) {
            free(tmp);
            return -1;
        }
        free(tmp);
        if (mkdir(path, 0755) == -1 && errno != EEXIST) {
            return -1;
        }
        return 0;
    }
    if (errno == EEXIST) {
        return 0;
    }
    return -1;
}

static int write_all(int fd, const void *buf, size_t size) {
    const unsigned char *p = (const unsigned char *)buf;
    size_t written = 0;
    while (written < size) {
        ssize_t rc = write(fd, p + written, size - written);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (rc == 0) {
            errno = EIO;
            return -1;
        }
        written += (size_t)rc;
    }
    return 0;
}

static char *normalize_path_copy(const char *path) {
    if (!path) {
        errno = EINVAL;
        return NULL;
    }
    if (path[0] == '/') {
        return strdup(path);
    }
    size_t len = strlen(path);
    char *copy = malloc(len + 2);
    if (!copy) {
        errno = ENOMEM;
        return NULL;
    }
    copy[0] = '/';
    memcpy(copy + 1, path, len + 1);
    return copy;
}

static const char *normalize_path_view(const char *path, char **allocated) {
    if (!path) {
        errno = EINVAL;
        return NULL;
    }
    if (path[0] == '/') {
        *allocated = NULL;
        return path;
    }
    char *copy = normalize_path_copy(path);
    if (!copy) {
        return NULL;
    }
    *allocated = copy;
    return copy;
}

static int read_all(int fd, void *buf, size_t size) {
    unsigned char *p = (unsigned char *)buf;
    size_t read_bytes = 0;
    while (read_bytes < size) {
        ssize_t rc = read(fd, p + read_bytes, size - read_bytes);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (rc == 0) {
            return -1;
        }
        read_bytes += (size_t)rc;
    }
    return 0;
}

static void free_inode(struct appendfs_inode *inode) {
    if (!inode) {
        return;
    }
    free(inode->path);
    free(inode->extents);
    free(inode->symlink_target);
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        free(inode->xattrs[i].name);
        free(inode->xattrs[i].value);
    }
    free(inode->xattrs);
}

static struct appendfs_inode *find_inode_by_path(struct appendfs_context *ctx, const char *path) {
    char *normalized = NULL;
    const char *needle = normalize_path_view(path, &normalized);
    if (!needle) {
        return NULL;
    }
    for (size_t i = 0; i < ctx->inode_count; ++i) {
        struct appendfs_inode *inode = &ctx->inodes[i];
        if (!inode->deleted && inode->path && strcmp(inode->path, needle) == 0) {
            free(normalized);
            return inode;
        }
        if (!inode->deleted && inode->path && inode->path[0] != '/' && needle[0] == '/' && strcmp(inode->path, needle + 1) == 0) {
            free(normalized);
            return inode;
        }
        if (!inode->deleted && inode->path && inode->path[0] == '/' && needle[0] != '/' && strcmp(inode->path + 1, needle) == 0) {
            free(normalized);
            return inode;
        }
    }
    free(normalized);
    return NULL;
}

static struct appendfs_inode *find_inode_by_id(struct appendfs_context *ctx, uint64_t inode_id) {
    for (size_t i = 0; i < ctx->inode_count; ++i) {
        if (ctx->inodes[i].inode_id == inode_id) {
            return &ctx->inodes[i];
        }
    }
    return NULL;
}

static int ensure_inode_capacity(struct appendfs_context *ctx) {
    if (ctx->inode_count >= ctx->inode_capacity) {
        size_t new_capacity = ctx->inode_capacity ? ctx->inode_capacity * 2 : 16;
        struct appendfs_inode *new_inodes = realloc(ctx->inodes, new_capacity * sizeof(*new_inodes));
        if (!new_inodes) {
            return -1;
        }
        for (size_t i = ctx->inode_capacity; i < new_capacity; ++i) {
            memset(&new_inodes[i], 0, sizeof(*new_inodes));
        }
        ctx->inodes = new_inodes;
        ctx->inode_capacity = new_capacity;
    }
    return 0;
}

static int add_extent(struct appendfs_inode *inode, off_t logical, off_t data_offset, uint32_t length) {
    if (inode->extent_count >= inode->extent_capacity) {
        size_t new_capacity = inode->extent_capacity ? inode->extent_capacity * 2 : 8;
        struct appendfs_extent *extents = realloc(inode->extents, new_capacity * sizeof(*extents));
        if (!extents) {
            return -1;
        }
        inode->extents = extents;
        inode->extent_capacity = new_capacity;
    }
    inode->extents[inode->extent_count].logical_offset = logical;
    inode->extents[inode->extent_count].data_offset = data_offset;
    inode->extents[inode->extent_count].length = length;
    inode->extent_count += 1;
    return 0;
}

static struct appendfs_xattr *find_xattr(struct appendfs_inode *inode, const char *name) {
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        if (strcmp(inode->xattrs[i].name, name) == 0) {
            return &inode->xattrs[i];
        }
    }
    return NULL;
}

static int ensure_xattr_capacity(struct appendfs_inode *inode) {
    if (inode->xattr_count >= inode->xattr_capacity) {
        size_t new_capacity = inode->xattr_capacity ? inode->xattr_capacity * 2 : 4;
        struct appendfs_xattr *items = realloc(inode->xattrs, new_capacity * sizeof(*items));
        if (!items) {
            return -1;
        }
        inode->xattrs = items;
        inode->xattr_capacity = new_capacity;
    }
    return 0;
}

static int set_xattr_internal(struct appendfs_inode *inode, const char *name, const void *value, size_t size, int flags) {
    struct appendfs_xattr *existing = find_xattr(inode, name);
    if (flags & XATTR_CREATE) {
        if (existing) {
            errno = EEXIST;
            return -1;
        }
    }
    if (flags & XATTR_REPLACE) {
        if (!existing) {
            errno = ENODATA;
            return -1;
        }
    }
    if (!existing) {
        if (ensure_xattr_capacity(inode) == -1) {
            return -1;
        }
        existing = &inode->xattrs[inode->xattr_count++];
        existing->name = strdup(name);
        if (!existing->name) {
            inode->xattr_count--;
            return -1;
        }
        existing->value = NULL;
        existing->size = 0;
    } else {
        unsigned char *new_value = realloc(existing->value, size);
        if (!new_value && size > 0) {
            return -1;
        }
        existing->value = new_value;
        existing->size = size;
        if (size > 0) {
            memcpy(existing->value, value, size);
        }
        return 0;
    }

    if (size > 0) {
        existing->value = malloc(size);
        if (!existing->value) {
            free(existing->name);
            existing->name = NULL;
            inode->xattr_count--;
            return -1;
        }
        memcpy(existing->value, value, size);
    } else {
        existing->value = NULL;
    }
    existing->size = size;
    return 0;
}

static int remove_xattr_internal(struct appendfs_inode *inode, const char *name) {
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        if (strcmp(inode->xattrs[i].name, name) == 0) {
            free(inode->xattrs[i].name);
            free(inode->xattrs[i].value);
            if (i + 1 < inode->xattr_count) {
                memmove(&inode->xattrs[i], &inode->xattrs[i + 1], (inode->xattr_count - i - 1) * sizeof(*inode->xattrs));
            }
            inode->xattr_count--;
            return 0;
        }
    }
    errno = ENODATA;
    return -1;
}

static int write_record(struct appendfs_context *ctx, uint8_t type, const void *payload, uint32_t length) {
    uint8_t header[RECORD_HEADER_SIZE];
    header[0] = type;
    header[1] = (uint8_t)(length & 0xffu);
    header[2] = (uint8_t)((length >> 8) & 0xffu);
    header[3] = (uint8_t)((length >> 16) & 0xffu);
    header[4] = (uint8_t)((length >> 24) & 0xffu);
    uint32_t checksum = appendfs_crc32(payload, length);
    header[5] = (uint8_t)(checksum & 0xffu);
    header[6] = (uint8_t)((checksum >> 8) & 0xffu);
    header[7] = (uint8_t)((checksum >> 16) & 0xffu);
    header[8] = (uint8_t)((checksum >> 24) & 0xffu);

    if (write_all(ctx->meta_fd, header, sizeof(header)) == -1) {
        return -1;
    }
    if (write_all(ctx->meta_fd, payload, length) == -1) {
        return -1;
    }
    return 0;
}

static int replay_metadata(struct appendfs_context *ctx) {
    if (lseek(ctx->meta_fd, 0, SEEK_SET) == (off_t)-1) {
        return -1;
    }
    uint8_t header[RECORD_HEADER_SIZE];
    while (1) {
        ssize_t hdr = read(ctx->meta_fd, header, sizeof(header));
        if (hdr == 0) {
            break;
        }
        if (hdr < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if ((size_t)hdr < sizeof(header)) {
            break;
        }
        uint8_t type = header[0];
        uint32_t length = (uint32_t)header[1] |
                          ((uint32_t)header[2] << 8) |
                          ((uint32_t)header[3] << 16) |
                          ((uint32_t)header[4] << 24);
        uint32_t checksum = (uint32_t)header[5] |
                            ((uint32_t)header[6] << 8) |
                            ((uint32_t)header[7] << 16) |
                            ((uint32_t)header[8] << 24);
        unsigned char *payload = malloc(length);
        if (!payload) {
            return -1;
        }
        if (read_all(ctx->meta_fd, payload, length) == -1) {
            free(payload);
            break;
        }
        uint32_t actual = appendfs_crc32(payload, length);
        if (actual != checksum) {
            free(payload);
            continue;
        }
        const unsigned char *p = payload;
        switch (type) {
        case APPENDFS_RECORD_CREATE:
        case APPENDFS_RECORD_MKDIR: {
            if (length < sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)) {
                break;
            }
            uint64_t inode_id = 0;
            uint32_t mode = 0;
            uint64_t size = 0;
            uint64_t ts = 0;
            uint32_t path_len = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&mode, p + sizeof(uint64_t), sizeof(uint32_t));
            memcpy(&size, p + sizeof(uint64_t) + sizeof(uint32_t), sizeof(uint64_t));
            memcpy(&ts, p + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t), sizeof(uint64_t));
            memcpy(&path_len, p + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t), sizeof(uint32_t));
            size_t offset = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);
            if (offset + path_len > length) {
                break;
            }
            char *path = strndup((const char *)(p + offset), path_len);
            if (!path) {
                break;
            }
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                if (ensure_inode_capacity(ctx) == -1) {
                    free(path);
                    break;
                }
                inode = &ctx->inodes[ctx->inode_count++];
                memset(inode, 0, sizeof(*inode));
                inode->inode_id = inode_id;
            } else {
                free(inode->path);
                inode->extent_count = 0;
                free(inode->symlink_target);
                inode->symlink_target = NULL;
                for (size_t i = 0; i < inode->xattr_count; ++i) {
                    free(inode->xattrs[i].name);
                    free(inode->xattrs[i].value);
                }
                inode->xattr_count = 0;
            }
            inode->path = path;
            inode->mode = (mode_t)mode;
            inode->size = (off_t)size;
            inode->ctime = (time_t)ts;
            inode->mtime = (time_t)ts;
            inode->atime = (time_t)ts;
            inode->deleted = 0;
            offset += path_len;
            if (S_ISLNK(inode->mode)) {
                if (offset + sizeof(uint32_t) <= length) {
                    uint32_t target_len = 0;
                    memcpy(&target_len, p + offset, sizeof(uint32_t));
                    offset += sizeof(uint32_t);
                    if (offset + target_len <= length) {
                        char *target = strndup((const char *)(p + offset), target_len);
                        if (target) {
                            inode->symlink_target = target;
                        }
                    }
                }
            }
            if (ctx->next_inode_id <= inode_id) {
                ctx->next_inode_id = inode_id + 1;
            }
            break;
        }
        case APPENDFS_RECORD_EXTENT: {
            if (length < sizeof(uint64_t) * 4 + sizeof(uint32_t)) {
                break;
            }
            uint64_t inode_id = 0;
            uint64_t logical_raw = 0;
            uint64_t data_raw = 0;
            uint32_t len = 0;
            uint64_t new_size_raw = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&logical_raw, p + sizeof(uint64_t), sizeof(uint64_t));
            memcpy(&data_raw, p + sizeof(uint64_t) * 2, sizeof(uint64_t));
            memcpy(&len, p + sizeof(uint64_t) * 3, sizeof(uint32_t));
            memcpy(&new_size_raw, p + sizeof(uint64_t) * 3 + sizeof(uint32_t), sizeof(uint64_t));
            off_t logical = (off_t)logical_raw;
            off_t data_offset = (off_t)data_raw;
            off_t new_size = (off_t)new_size_raw;
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            add_extent(inode, logical, data_offset, len);
            if (new_size > inode->size) {
                inode->size = new_size;
            }
            break;
        }
        case APPENDFS_RECORD_TRUNCATE: {
            if (length < sizeof(uint64_t) * 2) {
                break;
            }
            uint64_t inode_id = 0;
            uint64_t new_size_raw = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&new_size_raw, p + sizeof(uint64_t), sizeof(uint64_t));
            off_t new_size = (off_t)new_size_raw;
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            inode->size = new_size;
            for (size_t i = 0; i < inode->extent_count; ++i) {
                struct appendfs_extent *ext = &inode->extents[i];
                if (ext->logical_offset >= new_size) {
                    inode->extent_count = i;
                    break;
                }
                off_t end = ext->logical_offset + ext->length;
                if (end > new_size) {
                    ext->length = (uint32_t)(new_size - ext->logical_offset);
                    inode->extent_count = i + 1;
                    break;
                }
            }
            break;
        }
        case APPENDFS_RECORD_UNLINK: {
            if (length < sizeof(uint64_t)) {
                break;
            }
            uint64_t inode_id = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (inode) {
                inode->deleted = 1;
            }
            break;
        }
        case APPENDFS_RECORD_RENAME: {
            if (length < sizeof(uint64_t) + sizeof(uint32_t)) {
                break;
            }
            uint64_t inode_id = 0;
            uint32_t path_len = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&path_len, p + sizeof(uint64_t), sizeof(uint32_t));
            if (sizeof(uint64_t) + sizeof(uint32_t) + path_len > length) {
                break;
            }
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            char *path = strndup((const char *)(p + sizeof(uint64_t) + sizeof(uint32_t)), path_len);
            if (!path) {
                break;
            }
            free(inode->path);
            inode->path = path;
            inode->deleted = 0;
            break;
        }
        case APPENDFS_RECORD_SETXATTR: {
            if (length < sizeof(uint64_t) + sizeof(uint32_t) * 2) {
                break;
            }
            uint64_t inode_id = 0;
            uint32_t name_len = 0;
            uint32_t value_len = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&name_len, p + sizeof(uint64_t), sizeof(uint32_t));
            memcpy(&value_len, p + sizeof(uint64_t) + sizeof(uint32_t), sizeof(uint32_t));
            size_t offset = sizeof(uint64_t) + sizeof(uint32_t) * 2;
            if (offset + name_len + value_len > length) {
                break;
            }
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            char *name = strndup((const char *)(p + offset), name_len);
            if (!name) {
                break;
            }
            offset += name_len;
            const void *value = p + offset;
            set_xattr_internal(inode, name, value, value_len, 0);
            free(name);
            break;
        }
        case APPENDFS_RECORD_REMOVEXATTR: {
            if (length < sizeof(uint64_t) + sizeof(uint32_t)) {
                break;
            }
            uint64_t inode_id = 0;
            uint32_t name_len = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&name_len, p + sizeof(uint64_t), sizeof(uint32_t));
            if (sizeof(uint64_t) + sizeof(uint32_t) + name_len > length) {
                break;
            }
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            char *name = strndup((const char *)(p + sizeof(uint64_t) + sizeof(uint32_t)), name_len);
            if (!name) {
                break;
            }
            remove_xattr_internal(inode, name);
            free(name);
            break;
        }
        case APPENDFS_RECORD_TIMES: {
            if (length < sizeof(uint64_t) + sizeof(int64_t) * 2) {
                break;
            }
            uint64_t inode_id = 0;
            int64_t atime_raw = 0;
            int64_t mtime_raw = 0;
            memcpy(&inode_id, p, sizeof(uint64_t));
            memcpy(&atime_raw, p + sizeof(uint64_t), sizeof(int64_t));
            memcpy(&mtime_raw, p + sizeof(uint64_t) + sizeof(int64_t), sizeof(int64_t));
            struct appendfs_inode *inode = find_inode_by_id(ctx, inode_id);
            if (!inode) {
                break;
            }
            inode->atime = (time_t)atime_raw;
            inode->mtime = (time_t)mtime_raw;
            break;
        }
        default:
            break;
        }
        free(payload);
    }
    lseek(ctx->meta_fd, 0, SEEK_END);
    return 0;
}

static int append_create_record(struct appendfs_context *ctx, struct appendfs_inode *inode) {
    size_t path_len = strlen(inode->path);
    size_t payload_len = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + path_len;
    uint32_t target_len32 = 0;
    if (S_ISLNK(inode->mode) && inode->symlink_target) {
        target_len32 = (uint32_t)strlen(inode->symlink_target);
        payload_len += sizeof(uint32_t) + target_len32;
    }
    unsigned char *payload = malloc(payload_len);
    if (!payload) {
        return -1;
    }
    unsigned char *p = payload;
    memcpy(p, &inode->inode_id, sizeof(uint64_t));
    p += sizeof(uint64_t);
    uint32_t mode = (uint32_t)inode->mode;
    memcpy(p, &mode, sizeof(uint32_t));
    p += sizeof(uint32_t);
    uint64_t size = (uint64_t)inode->size;
    memcpy(p, &size, sizeof(uint64_t));
    p += sizeof(uint64_t);
    uint64_t ts = (uint64_t)inode->mtime;
    memcpy(p, &ts, sizeof(uint64_t));
    p += sizeof(uint64_t);
    uint32_t path_len32 = (uint32_t)path_len;
    memcpy(p, &path_len32, sizeof(uint32_t));
    p += sizeof(uint32_t);
    memcpy(p, inode->path, path_len);
    p += path_len;
    if (S_ISLNK(inode->mode) && inode->symlink_target) {
        memcpy(p, &target_len32, sizeof(uint32_t));
        p += sizeof(uint32_t);
        memcpy(p, inode->symlink_target, target_len32);
        p += target_len32;
    }

    int rc = write_record(ctx, S_ISDIR(inode->mode) ? APPENDFS_RECORD_MKDIR : APPENDFS_RECORD_CREATE, payload, (uint32_t)payload_len);
    free(payload);
    return rc;
}

static int append_extent_record(struct appendfs_context *ctx, struct appendfs_inode *inode, off_t logical, off_t data_offset, uint32_t length) {
    size_t payload_len = sizeof(uint64_t) * 3 + sizeof(uint32_t) + sizeof(uint64_t);
    unsigned char payload[sizeof(uint64_t) * 3 + sizeof(uint32_t) + sizeof(uint64_t)];
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    memcpy(payload + sizeof(uint64_t), &logical, sizeof(uint64_t));
    memcpy(payload + sizeof(uint64_t) * 2, &data_offset, sizeof(uint64_t));
    memcpy(payload + sizeof(uint64_t) * 3, &length, sizeof(uint32_t));
    off_t size = inode->size;
    memcpy(payload + sizeof(uint64_t) * 3 + sizeof(uint32_t), &size, sizeof(uint64_t));
    return write_record(ctx, APPENDFS_RECORD_EXTENT, payload, (uint32_t)payload_len);
}

static int append_truncate_record(struct appendfs_context *ctx, struct appendfs_inode *inode) {
    unsigned char payload[sizeof(uint64_t) * 2];
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    memcpy(payload + sizeof(uint64_t), &inode->size, sizeof(uint64_t));
    return write_record(ctx, APPENDFS_RECORD_TRUNCATE, payload, sizeof(payload));
}

static void clear_inode_xattrs(struct appendfs_inode *inode) {
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        free(inode->xattrs[i].name);
        free(inode->xattrs[i].value);
    }
    inode->xattr_count = 0;
}

static int path_is_prefix(const char *path, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    if (strncmp(path, prefix, prefix_len) != 0) {
        return 0;
    }
    if (prefix_len == 0) {
        return 1;
    }
    if (prefix[prefix_len - 1] == '/') {
        return 1;
    }
    return path[prefix_len] == '\0' || path[prefix_len] == '/';
}

static int is_immediate_child(const char *parent, const char *candidate, const char **name_out) {
    if (!parent || !candidate) {
        return 0;
    }
    if (strcmp(parent, "/") == 0) {
        if (candidate[0] != '/' || candidate[1] == '\0') {
            return 0;
        }
        const char *rest = candidate + 1;
        const char *slash = strchr(rest, '/');
        if (slash) {
            return 0;
        }
        if (name_out) {
            *name_out = rest;
        }
        return 1;
    }
    size_t parent_len = strlen(parent);
    if (strncmp(candidate, parent, parent_len) != 0) {
        return 0;
    }
    if (candidate[parent_len] != '/') {
        return 0;
    }
    const char *rest = candidate + parent_len + 1;
    if (*rest == '\0') {
        return 0;
    }
    const char *slash = strchr(rest, '/');
    if (slash) {
        return 0;
    }
    if (name_out) {
        *name_out = rest;
    }
    return 1;
}

static int split_path(const char *path, char **parent_out, char **name_out) {
    if (!path || path[0] != '/' || (path[1] == '\0' && parent_out)) {
        errno = EINVAL;
        return -1;
    }
    const char *slash = strrchr(path, '/');
    if (!slash) {
        errno = EINVAL;
        return -1;
    }
    const char *name = slash + 1;
    if (*name == '\0') {
        errno = EINVAL;
        return -1;
    }
    size_t parent_len = (slash == path) ? 1 : (size_t)(slash - path);
    if (parent_out) {
        char *parent = malloc(parent_len + 1);
        if (!parent) {
            return -1;
        }
        if (slash == path) {
            parent[0] = '/';
            parent[1] = '\0';
        } else {
            memcpy(parent, path, parent_len);
            parent[parent_len] = '\0';
        }
        *parent_out = parent;
    }
    if (name_out) {
        *name_out = strdup(name);
        if (!*name_out) {
            if (parent_out && *parent_out) {
                free(*parent_out);
                *parent_out = NULL;
            }
            return -1;
        }
    }
    return 0;
}

static int append_unlink_record(struct appendfs_context *ctx, struct appendfs_inode *inode) {
    unsigned char payload[sizeof(uint64_t)];
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    return write_record(ctx, APPENDFS_RECORD_UNLINK, payload, sizeof(payload));
}

static int append_rename_record(struct appendfs_context *ctx, struct appendfs_inode *inode, const char *new_path) {
    size_t path_len = strlen(new_path);
    size_t payload_len = sizeof(uint64_t) + sizeof(uint32_t) + path_len;
    unsigned char *payload = malloc(payload_len);
    if (!payload) {
        return -1;
    }
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    uint32_t path_len32 = (uint32_t)path_len;
    memcpy(payload + sizeof(uint64_t), &path_len32, sizeof(uint32_t));
    memcpy(payload + sizeof(uint64_t) + sizeof(uint32_t), new_path, path_len);
    int rc = write_record(ctx, APPENDFS_RECORD_RENAME, payload, (uint32_t)payload_len);
    free(payload);
    return rc;
}

static int append_setxattr_record(struct appendfs_context *ctx, struct appendfs_inode *inode, const char *name, const void *value, size_t size) {
    size_t name_len = strlen(name);
    size_t payload_len = sizeof(uint64_t) + sizeof(uint32_t) * 2 + name_len + size;
    unsigned char *payload = malloc(payload_len);
    if (!payload) {
        return -1;
    }
    unsigned char *p = payload;
    memcpy(p, &inode->inode_id, sizeof(uint64_t));
    p += sizeof(uint64_t);
    uint32_t name_len32 = (uint32_t)name_len;
    uint32_t value_len32 = (uint32_t)size;
    memcpy(p, &name_len32, sizeof(uint32_t));
    p += sizeof(uint32_t);
    memcpy(p, &value_len32, sizeof(uint32_t));
    p += sizeof(uint32_t);
    memcpy(p, name, name_len);
    p += name_len;
    if (size > 0) {
        memcpy(p, value, size);
    }
    int rc = write_record(ctx, APPENDFS_RECORD_SETXATTR, payload, (uint32_t)payload_len);
    free(payload);
    return rc;
}

static int append_removexattr_record(struct appendfs_context *ctx, struct appendfs_inode *inode, const char *name) {
    size_t name_len = strlen(name);
    size_t payload_len = sizeof(uint64_t) + sizeof(uint32_t) + name_len;
    unsigned char *payload = malloc(payload_len);
    if (!payload) {
        return -1;
    }
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    uint32_t name_len32 = (uint32_t)name_len;
    memcpy(payload + sizeof(uint64_t), &name_len32, sizeof(uint32_t));
    memcpy(payload + sizeof(uint64_t) + sizeof(uint32_t), name, name_len);
    int rc = write_record(ctx, APPENDFS_RECORD_REMOVEXATTR, payload, (uint32_t)payload_len);
    free(payload);
    return rc;
}

static int append_times_record(struct appendfs_context *ctx, struct appendfs_inode *inode) {
    unsigned char payload[sizeof(uint64_t) + sizeof(int64_t) * 2];
    int64_t atime_raw = (int64_t)inode->atime;
    int64_t mtime_raw = (int64_t)inode->mtime;
    memcpy(payload, &inode->inode_id, sizeof(uint64_t));
    memcpy(payload + sizeof(uint64_t), &atime_raw, sizeof(int64_t));
    memcpy(payload + sizeof(uint64_t) + sizeof(int64_t), &mtime_raw, sizeof(int64_t));
    return write_record(ctx, APPENDFS_RECORD_TIMES, payload, sizeof(payload));
}

int appendfs_open(const char *root_path, struct appendfs_context **out_ctx) {
    if (!root_path || !out_ctx) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_context *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return -1;
    }
    ctx->data_fd = -1;
    ctx->meta_fd = -1;
    ctx->write_buffer_size = APPENDFS_DEFAULT_BUFFER;
    ctx->next_inode_id = 1;
    ctx->root_path = realpath(root_path, NULL);
    if (!ctx->root_path) {
        if (errno == ENOENT) {
            if (ensure_directory(root_path) == -1) {
                free(ctx);
                return -1;
            }
            ctx->root_path = realpath(root_path, NULL);
            if (!ctx->root_path) {
                free(ctx);
                return -1;
            }
        } else {
            free(ctx);
            return -1;
        }
    }

    char data_path[PATH_MAX];
    char meta_path[PATH_MAX];
    snprintf(data_path, sizeof(data_path), "%s/%s", ctx->root_path, DATA_FILENAME);
    snprintf(meta_path, sizeof(meta_path), "%s/%s", ctx->root_path, META_FILENAME);

    ctx->data_fd = open(data_path, O_RDWR | O_CREAT | O_BINARY, 0644);
    if (ctx->data_fd == -1) {
        appendfs_close(ctx);
        return -1;
    }
    ctx->meta_fd = open(meta_path, O_RDWR | O_CREAT | O_BINARY, 0644);
    if (ctx->meta_fd == -1) {
        appendfs_close(ctx);
        return -1;
    }
    if (replay_metadata(ctx) == -1) {
        appendfs_close(ctx);
        return -1;
    }

    *out_ctx = ctx;
    return 0;
}

void appendfs_close(struct appendfs_context *ctx) {
    if (!ctx) {
        return;
    }
    if (ctx->data_fd != -1) {
        close(ctx->data_fd);
    }
    if (ctx->meta_fd != -1) {
        close(ctx->meta_fd);
    }
    for (size_t i = 0; i < ctx->inode_count; ++i) {
        free_inode(&ctx->inodes[i]);
    }
    free(ctx->inodes);
    free(ctx->root_path);
    free(ctx);
}

int appendfs_set_options(struct appendfs_context *ctx, const struct appendfs_options *opts) {
    if (!ctx || !opts) {
        errno = EINVAL;
        return -1;
    }
    if (opts->write_buffer_size < APPENDFS_MIN_FLUSH) {
        errno = EINVAL;
        return -1;
    }
    ctx->write_buffer_size = opts->write_buffer_size;
    return 0;
}

static struct appendfs_inode *create_inode(struct appendfs_context *ctx, const char *path, mode_t mode) {
    if (ensure_inode_capacity(ctx) == -1) {
        return NULL;
    }
    struct appendfs_inode *inode = &ctx->inodes[ctx->inode_count++];
    memset(inode, 0, sizeof(*inode));
    inode->inode_id = ctx->next_inode_id++;
    inode->path = normalize_path_copy(path);
    if (!inode->path) {
        ctx->inode_count--;
        return NULL;
    }
    inode->mode = mode;
    inode->ctime = inode->mtime = inode->atime = time(NULL);
    inode->size = 0;
    inode->deleted = 0;
    return inode;
}

int appendfs_create_file(struct appendfs_context *ctx, const char *path, mode_t mode) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    char *norm_path = normalize_path_copy(path);
    if (!norm_path) {
        return -1;
    }
    struct appendfs_inode *existing = find_inode_by_path(ctx, norm_path);
    if (existing && !existing->deleted) {
        free(norm_path);
        errno = EEXIST;
        return -1;
    }
    char *parent = NULL;
    if (split_path(norm_path, &parent, NULL) == -1) {
        free(norm_path);
        return -1;
    }
    if (parent && strcmp(parent, "/") != 0) {
        struct appendfs_inode *parent_inode = find_inode_by_path(ctx, parent);
        if (!parent_inode || parent_inode->deleted || !S_ISDIR(parent_inode->mode)) {
            free(parent);
            free(norm_path);
            errno = ENOENT;
            return -1;
        }
    }
    free(parent);
    struct appendfs_inode *inode = existing;
    if (existing && existing->deleted) {
        free(existing->path);
        existing->path = strdup(norm_path);
        if (!existing->path) {
            free(norm_path);
            return -1;
        }
        existing->extent_count = 0;
        existing->size = 0;
        existing->deleted = 0;
        clear_inode_xattrs(existing);
        inode = existing;
    } else if (!inode) {
        inode = create_inode(ctx, norm_path, S_IFREG | mode);
        if (!inode) {
            free(norm_path);
            return -1;
        }
    }
    inode->mode = S_IFREG | mode;
    inode->ctime = inode->mtime = inode->atime = time(NULL);
    if (append_create_record(ctx, inode) == -1) {
        if (!existing) {
            free_inode(inode);
            memset(inode, 0, sizeof(*inode));
            ctx->inode_count--;
        }
        free(norm_path);
        return -1;
    }
    free(norm_path);
    return 0;
}

int appendfs_symlink(struct appendfs_context *ctx, const char *target, const char *linkpath, mode_t mode) {
    (void)mode;
    if (!ctx || !target || !linkpath) {
        errno = EINVAL;
        return -1;
    }
    char *norm_path = normalize_path_copy(linkpath);
    if (!norm_path) {
        return -1;
    }
    struct appendfs_inode *existing = find_inode_by_path(ctx, norm_path);
    if (existing && !existing->deleted) {
        free(norm_path);
        errno = EEXIST;
        return -1;
    }
    char *parent = NULL;
    if (split_path(norm_path, &parent, NULL) == -1) {
        free(norm_path);
        return -1;
    }
    if (parent && strcmp(parent, "/") != 0) {
        struct appendfs_inode *parent_inode = find_inode_by_path(ctx, parent);
        if (!parent_inode || parent_inode->deleted || !S_ISDIR(parent_inode->mode)) {
            free(parent);
            free(norm_path);
            errno = ENOENT;
            return -1;
        }
    }
    free(parent);
    struct appendfs_inode *inode = existing;
    time_t now = time(NULL);
    if (existing && existing->deleted) {
        char *new_path = strdup(norm_path);
        if (!new_path) {
            free(norm_path);
            return -1;
        }
        free(existing->path);
        existing->path = new_path;
        free(existing->symlink_target);
        existing->symlink_target = NULL;
        existing->extent_count = 0;
        clear_inode_xattrs(existing);
        inode = existing;
    } else if (!inode) {
        inode = create_inode(ctx, norm_path, S_IFLNK | 0777);
        if (!inode) {
            free(norm_path);
            return -1;
        }
    }
    free(inode->symlink_target);
    inode->symlink_target = strdup(target);
    if (!inode->symlink_target) {
        if (inode != existing) {
            free_inode(inode);
            memset(inode, 0, sizeof(*inode));
            ctx->inode_count--;
        }
        free(norm_path);
        return -1;
    }
    inode->mode = S_IFLNK | 0777;
    inode->size = (off_t)strlen(target);
    inode->ctime = inode->mtime = inode->atime = now;
    inode->deleted = 0;
    if (append_create_record(ctx, inode) == -1) {
        if (inode->symlink_target) {
            free(inode->symlink_target);
            inode->symlink_target = NULL;
        }
        if (!existing) {
            free_inode(inode);
            memset(inode, 0, sizeof(*inode));
            ctx->inode_count--;
        }
        free(norm_path);
        return -1;
    }
    free(norm_path);
    return 0;
}

ssize_t appendfs_readlink(struct appendfs_context *ctx, const char *path, char *buf, size_t size) {
    if (!ctx || !path || !buf) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    if (!S_ISLNK(inode->mode)) {
        errno = EINVAL;
        return -1;
    }
    size_t target_len = inode->symlink_target ? strlen(inode->symlink_target) : 0;
    if (size == 0) {
        return (ssize_t)target_len;
    }
    size_t copy_len = target_len < size - 1 ? target_len : size - 1;
    if (size == 1) {
        buf[0] = '\0';
        return (ssize_t)target_len;
    }
    if (inode->symlink_target) {
        memcpy(buf, inode->symlink_target, copy_len);
    }
    buf[copy_len] = '\0';
    inode->atime = time(NULL);
    return (ssize_t)target_len;
}

int appendfs_mkdirs(struct appendfs_context *ctx, const char *path, mode_t mode) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    if (find_inode_by_path(ctx, path)) {
        return 0;
    }
    struct appendfs_inode *inode = create_inode(ctx, path, S_IFDIR | mode);
    if (!inode) {
        return -1;
    }
    if (append_create_record(ctx, inode) == -1) {
        free_inode(inode);
        memset(inode, 0, sizeof(*inode));
        ctx->inode_count--;
        return -1;
    }
    return 0;
}

int appendfs_mkdir(struct appendfs_context *ctx, const char *path, mode_t mode) {
    if (!ctx || !path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }
    if (strcmp(path, "/") == 0) {
        errno = EINVAL;
        return -1;
    }
    char *norm_path = normalize_path_copy(path);
    if (!norm_path) {
        return -1;
    }
    struct appendfs_inode *existing = find_inode_by_path(ctx, norm_path);
    if (existing && !existing->deleted) {
        free(norm_path);
        errno = EEXIST;
        return -1;
    }
    char *parent = NULL;
    if (split_path(norm_path, &parent, NULL) == -1) {
        free(norm_path);
        return -1;
    }
    int rc = 0;
    if (parent && strcmp(parent, "/") != 0) {
        struct appendfs_inode *parent_inode = find_inode_by_path(ctx, parent);
        if (!parent_inode || parent_inode->deleted || !S_ISDIR(parent_inode->mode)) {
            errno = ENOENT;
            rc = -1;
            goto out;
        }
    }
    if (existing && existing->deleted) {
        existing->deleted = 0;
        existing->mode = S_IFDIR | (mode & 0777);
        existing->ctime = existing->mtime = existing->atime = time(NULL);
        clear_inode_xattrs(existing);
        rc = append_create_record(ctx, existing);
        goto out;
    }
    struct appendfs_inode *inode = create_inode(ctx, norm_path, S_IFDIR | (mode & 0777));
    if (!inode) {
        rc = -1;
        goto out;
    }
    if (append_create_record(ctx, inode) == -1) {
        free_inode(inode);
        memset(inode, 0, sizeof(*inode));
        ctx->inode_count--;
        rc = -1;
        goto out;
    }
    rc = 0;
out:
    free(parent);
    free(norm_path);
    return rc;
}

int appendfs_unlink(struct appendfs_context *ctx, const char *path) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode) {
        errno = ENOENT;
        return -1;
    }
    if (S_ISDIR(inode->mode)) {
        errno = EISDIR;
        return -1;
    }
    inode->deleted = 1;
    if (append_unlink_record(ctx, inode) == -1) {
        return -1;
    }
    return 0;
}

int appendfs_rmdir(struct appendfs_context *ctx, const char *path) {
    if (!ctx || !path || strcmp(path, "/") == 0) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    if (!S_ISDIR(inode->mode)) {
        errno = ENOTDIR;
        return -1;
    }
    int empty = appendfs_is_directory_empty(ctx, path);
    if (empty < 0) {
        return -1;
    }
    if (!empty) {
        errno = ENOTEMPTY;
        return -1;
    }
    inode->deleted = 1;
    inode->mtime = time(NULL);
    if (append_unlink_record(ctx, inode) == -1) {
        return -1;
    }
    return 0;
}

int appendfs_rename(struct appendfs_context *ctx, const char *from_path, const char *to_path) {
    if (!ctx || !from_path || !to_path) {
        errno = EINVAL;
        return -1;
    }
    char *from_norm = normalize_path_copy(from_path);
    if (!from_norm) {
        return -1;
    }
    char *to_norm = normalize_path_copy(to_path);
    if (!to_norm) {
        free(from_norm);
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, from_norm);
    if (!inode) {
        free(from_norm);
        free(to_norm);
        errno = ENOENT;
        return -1;
    }
    if (strcmp(from_norm, to_norm) == 0) {
        free(from_norm);
        free(to_norm);
        return 0;
    }
    char *to_parent = NULL;
    if (split_path(to_norm, &to_parent, NULL) == -1) {
        free(from_norm);
        free(to_norm);
        return -1;
    }
    if (to_parent && strcmp(to_parent, "/") != 0) {
        struct appendfs_inode *parent = find_inode_by_path(ctx, to_parent);
        if (!parent || parent->deleted || !S_ISDIR(parent->mode)) {
            free(to_parent);
            free(from_norm);
            free(to_norm);
            errno = ENOENT;
            return -1;
        }
    }
    struct appendfs_inode *dest = find_inode_by_path(ctx, to_norm);
    if (dest && !dest->deleted) {
        if (S_ISDIR(inode->mode)) {
            if (!S_ISDIR(dest->mode)) {
                free(to_parent);
                free(from_norm);
                free(to_norm);
                errno = ENOTDIR;
                return -1;
            }
            int empty = appendfs_is_directory_empty(ctx, to_norm);
            if (empty < 0) {
                free(to_parent);
                free(from_norm);
                free(to_norm);
                return -1;
            }
            if (!empty) {
                free(to_parent);
                free(from_norm);
                free(to_norm);
                errno = ENOTEMPTY;
                return -1;
            }
        } else {
            if (S_ISDIR(dest->mode)) {
                free(to_parent);
                free(from_norm);
                free(to_norm);
                errno = EISDIR;
                return -1;
            }
        }
        dest->deleted = 1;
        dest->mtime = time(NULL);
        if (append_unlink_record(ctx, dest) == -1) {
            free(to_parent);
            free(from_norm);
            free(to_norm);
            return -1;
        }
    }

    struct rename_child {
        struct appendfs_inode *inode;
        char *new_path;
    };
    struct rename_child *children = NULL;
    size_t child_count = 0;
    size_t child_capacity = 0;
    size_t from_len = strlen(from_norm);
    size_t to_len = strlen(to_norm);

    if (S_ISDIR(inode->mode)) {
        for (size_t i = 0; i < ctx->inode_count; ++i) {
            struct appendfs_inode *child = &ctx->inodes[i];
            if (child == inode || child->deleted) {
                continue;
            }
            if (!path_is_prefix(child->path, from_norm)) {
                continue;
            }
            if (strlen(child->path) <= from_len || child->path[from_len] != '/') {
                continue;
            }
            const char *suffix = child->path + from_len;
            size_t new_len = to_len + strlen(suffix);
            char *child_new_path = malloc(new_len + 1);
            if (!child_new_path) {
                free(to_parent);
                for (size_t j = 0; j < child_count; ++j) {
                    free(children[j].new_path);
                }
                free(children);
                return -1;
            }
            memcpy(child_new_path, to_norm, to_len);
            memcpy(child_new_path + to_len, suffix, strlen(suffix) + 1);
            if (child_count >= child_capacity) {
                size_t new_cap = child_capacity ? child_capacity * 2 : 4;
                struct rename_child *tmp = realloc(children, new_cap * sizeof(*tmp));
                if (!tmp) {
                    free(child_new_path);
                    free(to_parent);
                    for (size_t j = 0; j < child_count; ++j) {
                        free(children[j].new_path);
                    }
                    free(children);
                    free(from_norm);
                    free(to_norm);
                    return -1;
                }
                children = tmp;
                child_capacity = new_cap;
            }
            children[child_count].inode = child;
            children[child_count].new_path = child_new_path;
            child_count++;
        }
    }

    if (append_rename_record(ctx, inode, to_norm) == -1) {
        for (size_t j = 0; j < child_count; ++j) {
            free(children[j].new_path);
        }
        free(children);
        free(to_parent);
        free(from_norm);
        free(to_norm);
        return -1;
    }
    char *new_path = strdup(to_norm);
    if (!new_path) {
        for (size_t j = 0; j < child_count; ++j) {
            free(children[j].new_path);
        }
        free(children);
        free(to_parent);
        free(from_norm);
        free(to_norm);
        return -1;
    }
    char *old_path = inode->path;
    inode->path = new_path;
    inode->deleted = 0;
    inode->mtime = time(NULL);
    for (size_t i = 0; i < child_count; ++i) {
        struct appendfs_inode *child = children[i].inode;
        char *child_new_path = children[i].new_path;
        if (append_rename_record(ctx, child, child_new_path) == -1) {
            free(child_new_path);
            for (size_t j = i + 1; j < child_count; ++j) {
                free(children[j].new_path);
            }
            free(children);
            free(old_path);
            free(to_parent);
            free(from_norm);
            free(to_norm);
            return -1;
        }
        free(child->path);
        child->path = child_new_path;
        child->deleted = 0;
    }
    free(children);
    free(old_path);
    free(to_parent);
    free(from_norm);
    free(to_norm);
    return 0;
}

int appendfs_is_directory_empty(struct appendfs_context *ctx, const char *path) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    char *normalized = NULL;
    const char *dir_path = normalize_path_view(path, &normalized);
    if (!dir_path) {
        return -1;
    }
    for (size_t i = 0; i < ctx->inode_count; ++i) {
        struct appendfs_inode *inode = &ctx->inodes[i];
        if (inode->deleted) {
            continue;
        }
        if (strcmp(inode->path, dir_path) == 0) {
            continue;
        }
        if (is_immediate_child(dir_path, inode->path, NULL)) {
            free(normalized);
            return 0;
        }
    }
    free(normalized);
    return 1;
}

int appendfs_iterate_children(struct appendfs_context *ctx, const char *dir_path, appendfs_dir_iter_cb cb, void *user_data) {
    if (!ctx || !dir_path || !cb) {
        errno = EINVAL;
        return -1;
    }
    char *normalized = NULL;
    const char *parent = normalize_path_view(dir_path, &normalized);
    if (!parent) {
        return -1;
    }
    for (size_t i = 0; i < ctx->inode_count; ++i) {
        struct appendfs_inode *inode = &ctx->inodes[i];
        if (inode->deleted) {
            continue;
        }
        const char *name = NULL;
        if (!is_immediate_child(parent, inode->path, &name)) {
            continue;
        }
        struct appendfs_inode_info info;
        info.inode_id = inode->inode_id;
        info.mode = inode->mode;
        info.size = inode->size;
        info.ctime = inode->ctime;
        info.mtime = inode->mtime;
        info.atime = inode->atime;
        if (cb(name, &info, user_data) != 0) {
            break;
        }
    }
    free(normalized);
    return 0;
}

struct appendfs_file *appendfs_open_file(struct appendfs_context *ctx, const char *path, int flags, mode_t mode) {
    if (!ctx || !path) {
        errno = EINVAL;
        return NULL;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode) {
        if (!(flags & O_CREAT)) {
            errno = ENOENT;
            return NULL;
        }
        if (appendfs_create_file(ctx, path, mode) == -1) {
            return NULL;
        }
        inode = find_inode_by_path(ctx, path);
    }
    if (inode && S_ISDIR(inode->mode)) {
        errno = EISDIR;
        return NULL;
    }
    struct appendfs_file *file = calloc(1, sizeof(*file));
    if (!file) {
        return NULL;
    }
    file->ctx = ctx;
    file->inode = inode;
    file->buffer_size = ctx->write_buffer_size;
    file->buffer = malloc(file->buffer_size);
    if (!file->buffer) {
        free(file);
        return NULL;
    }
    file->buffer_used = 0;
    file->buffer_offset = 0;
    file->flags = flags;
    file->position = 0;
    if (flags & O_TRUNC) {
        if (appendfs_truncate(ctx, path, 0) == -1) {
            free(file->buffer);
            free(file);
            return NULL;
        }
    }
    if (flags & O_APPEND) {
        file->position = inode->size;
    }
    return file;
}

static int flush_buffer(struct appendfs_file *file) {
    if (!file || file->buffer_used == 0) {
        return 0;
    }
    struct appendfs_context *ctx = file->ctx;
    struct appendfs_inode *inode = file->inode;
    off_t data_offset = lseek(ctx->data_fd, 0, SEEK_END);
    if (data_offset == (off_t)-1) {
        return -1;
    }
    if (write_all(ctx->data_fd, file->buffer, file->buffer_used) == -1) {
        return -1;
    }
    if (add_extent(inode, file->buffer_offset, data_offset, (uint32_t)file->buffer_used) == -1) {
        return -1;
    }
    off_t new_size = file->buffer_offset + (off_t)file->buffer_used;
    if (new_size > inode->size) {
        inode->size = new_size;
    }
    inode->mtime = time(NULL);
    if (append_extent_record(ctx, inode, file->buffer_offset, data_offset, (uint32_t)file->buffer_used) == -1) {
        return -1;
    }
    file->buffer_used = 0;
    return 0;
}

ssize_t appendfs_write(struct appendfs_file *file, const void *buf, size_t size, off_t offset) {
    if (!file || !buf) {
        errno = EINVAL;
        return -1;
    }
    if (size == 0) {
        return 0;
    }
    if (file->buffer_used > 0 && offset != file->buffer_offset + (off_t)file->buffer_used) {
        if (flush_buffer(file) == -1) {
            return -1;
        }
    }
    if (file->buffer_used == 0) {
        file->buffer_offset = offset;
    }
    const unsigned char *p = buf;
    size_t remaining = size;
    while (remaining > 0) {
        size_t space = file->buffer_size - file->buffer_used;
        if (space == 0) {
            if (flush_buffer(file) == -1) {
                return -1;
            }
            file->buffer_offset = offset + (size - remaining);
            space = file->buffer_size;
        }
        size_t to_copy = remaining;
        if (to_copy > space) {
            to_copy = space;
        }
        memcpy(file->buffer + file->buffer_used, p + (size - remaining), to_copy);
        file->buffer_used += to_copy;
        remaining -= to_copy;
        if (file->buffer_used >= APPENDFS_MIN_FLUSH && file->buffer_used >= file->buffer_size) {
            if (flush_buffer(file) == -1) {
                return -1;
            }
            file->buffer_offset = offset + (size - remaining);
        }
    }
    file->position = offset + (off_t)size;
    return (ssize_t)size;
}

int appendfs_flush(struct appendfs_file *file) {
    if (!file) {
        errno = EINVAL;
        return -1;
    }
    if (flush_buffer(file) == -1) {
        return -1;
    }
    return 0;
}

int appendfs_close_file(struct appendfs_file *file) {
    if (!file) {
        errno = EINVAL;
        return -1;
    }
    int rc = appendfs_flush(file);
    free(file->buffer);
    free(file);
    return rc;
}

int appendfs_setxattr(struct appendfs_context *ctx, const char *path, const char *name, const void *value, size_t size, int flags) {
    if (!ctx || !path || !name || (size > 0 && !value)) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    struct appendfs_xattr *existing = find_xattr(inode, name);
    unsigned char *old_value = NULL;
    size_t old_size = 0;
    int had_existing = existing != NULL;
    if (had_existing && existing->size > 0) {
        old_value = malloc(existing->size);
        if (!old_value) {
            return -1;
        }
        memcpy(old_value, existing->value, existing->size);
        old_size = existing->size;
    }
    if (set_xattr_internal(inode, name, value, size, flags) == -1) {
        free(old_value);
        return -1;
    }
    if (append_setxattr_record(ctx, inode, name, value, size) == -1) {
        if (had_existing) {
            set_xattr_internal(inode, name, old_value, old_size, XATTR_REPLACE);
        } else {
            remove_xattr_internal(inode, name);
        }
        free(old_value);
        return -1;
    }
    free(old_value);
    return 0;
}

ssize_t appendfs_getxattr(struct appendfs_context *ctx, const char *path, const char *name, void *value, size_t size) {
    if (!ctx || !path || !name) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    struct appendfs_xattr *attr = find_xattr(inode, name);
    if (!attr) {
        errno = ENODATA;
        return -1;
    }
    if (!value) {
        return (ssize_t)attr->size;
    }
    if (size < attr->size) {
        errno = ERANGE;
        return -1;
    }
    if (attr->size > 0) {
        memcpy(value, attr->value, attr->size);
    }
    return (ssize_t)attr->size;
}

ssize_t appendfs_listxattr(struct appendfs_context *ctx, const char *path, char *list, size_t size) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    size_t total = 0;
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        total += strlen(inode->xattrs[i].name) + 1;
    }
    if (!list) {
        return (ssize_t)total;
    }
    if (size < total) {
        errno = ERANGE;
        return -1;
    }
    size_t offset = 0;
    for (size_t i = 0; i < inode->xattr_count; ++i) {
        size_t len = strlen(inode->xattrs[i].name);
        memcpy(list + offset, inode->xattrs[i].name, len);
        offset += len;
        list[offset++] = '\0';
    }
    return (ssize_t)total;
}

int appendfs_removexattr(struct appendfs_context *ctx, const char *path, const char *name) {
    if (!ctx || !path || !name) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    struct appendfs_xattr *attr = find_xattr(inode, name);
    if (!attr) {
        errno = ENODATA;
        return -1;
    }
    unsigned char *backup = NULL;
    size_t backup_size = attr->size;
    if (backup_size > 0) {
        backup = malloc(backup_size);
        if (!backup) {
            return -1;
        }
        memcpy(backup, attr->value, backup_size);
    }
    if (remove_xattr_internal(inode, name) == -1) {
        free(backup);
        return -1;
    }
    if (append_removexattr_record(ctx, inode, name) == -1) {
        set_xattr_internal(inode, name, backup, backup_size, XATTR_CREATE);
        free(backup);
        return -1;
    }
    free(backup);
    return 0;
}

int appendfs_fsync(struct appendfs_file *file, int datasync) {
    if (!file) {
        errno = EINVAL;
        return -1;
    }
    if (appendfs_flush(file) == -1) {
        return -1;
    }
    int data_fd = file->ctx->data_fd;
    if (fsync(data_fd) == -1) {
        return -1;
    }
    if (!datasync) {
        if (fsync(file->ctx->meta_fd) == -1) {
            return -1;
        }
    }
    return 0;
}

int appendfs_fsyncdir(struct appendfs_context *ctx) {
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    if (fsync(ctx->meta_fd) == -1) {
        return -1;
    }
    return 0;
}

off_t appendfs_seek(struct appendfs_file *file, off_t offset, int whence) {
    if (!file) {
        errno = EINVAL;
        return (off_t)-1;
    }
    if (file->buffer_used > 0) {
        if (appendfs_flush(file) == -1) {
            return (off_t)-1;
        }
    }
    struct appendfs_inode *inode = file->inode;
    switch (whence) {
    case SEEK_SET:
    case SEEK_CUR:
    case SEEK_END: {
        off_t base = 0;
        if (whence == SEEK_CUR) {
            base = file->position;
        } else if (whence == SEEK_END) {
            base = inode->size;
        }
        off_t new_pos = base + offset;
        if (new_pos < 0) {
            errno = EINVAL;
            return (off_t)-1;
        }
        file->position = new_pos;
        return new_pos;
    }
#ifdef SEEK_DATA
    case SEEK_DATA: {
        if (offset < 0) {
            errno = EINVAL;
            return (off_t)-1;
        }
        if (offset >= inode->size) {
            errno = ENXIO;
            return (off_t)-1;
        }
        off_t result = -1;
        for (size_t i = 0; i < inode->extent_count; ++i) {
            struct appendfs_extent *ext = &inode->extents[i];
            off_t start = ext->logical_offset;
            off_t end = ext->logical_offset + ext->length;
            if (end <= offset) {
                continue;
            }
            if (offset < start) {
                result = start;
            } else {
                result = offset;
            }
            break;
        }
        if (result < 0) {
            errno = ENXIO;
            return (off_t)-1;
        }
        file->position = result;
        return result;
    }
#endif
#ifdef SEEK_HOLE
    case SEEK_HOLE: {
        if (offset < 0) {
            errno = EINVAL;
            return (off_t)-1;
        }
        if (offset >= inode->size) {
            file->position = inode->size;
            return inode->size;
        }
        off_t pos = offset;
        for (size_t i = 0; i < inode->extent_count; ++i) {
            struct appendfs_extent *ext = &inode->extents[i];
            off_t start = ext->logical_offset;
            off_t end = ext->logical_offset + ext->length;
            if (pos < start) {
                file->position = pos;
                return pos;
            }
            if (pos >= start && pos < end) {
                pos = end;
            }
        }
        if (pos > inode->size) {
            pos = inode->size;
        }
        file->position = pos;
        return pos;
    }
#endif
    default:
        errno = EINVAL;
        return (off_t)-1;
    }
}

int appendfs_truncate(struct appendfs_context *ctx, const char *path, off_t size) {
    if (!ctx || !path) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode) {
        errno = ENOENT;
        return -1;
    }
    if (!S_ISREG(inode->mode) && !S_ISLNK(inode->mode)) {
        errno = EINVAL;
        return -1;
    }
    inode->size = size;
    if (append_truncate_record(ctx, inode) == -1) {
        return -1;
    }
    for (size_t i = 0; i < inode->extent_count; ++i) {
        struct appendfs_extent *ext = &inode->extents[i];
        if (ext->logical_offset >= size) {
            inode->extent_count = i;
            break;
        }
        off_t end = ext->logical_offset + ext->length;
        if (end > size) {
            ext->length = (uint32_t)(size - ext->logical_offset);
            inode->extent_count = i + 1;
            break;
        }
    }
    inode->mtime = time(NULL);
    return 0;
}

int appendfs_set_times(struct appendfs_context *ctx, const char *path, const struct timespec times[2]) {
    if (!ctx || !path || !times) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    time_t now = time(NULL);
    struct timespec atime = times[0];
    struct timespec mtime = times[1];
    if (atime.tv_nsec == UTIME_NOW) {
        inode->atime = now;
    } else if (atime.tv_nsec != UTIME_OMIT) {
        inode->atime = atime.tv_sec;
    }
    if (mtime.tv_nsec == UTIME_NOW) {
        inode->mtime = now;
    } else if (mtime.tv_nsec != UTIME_OMIT) {
        inode->mtime = mtime.tv_sec;
    }
    inode->ctime = now;
    if (append_times_record(ctx, inode) == -1) {
        return -1;
    }
    return 0;
}

ssize_t appendfs_read(struct appendfs_context *ctx, const char *path, void *buf, size_t size, off_t offset) {
    if (!ctx || !path || !buf) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    if (offset >= inode->size) {
        return 0;
    }
    unsigned char *out = buf;
    size_t total = 0;
    for (size_t i = 0; i < inode->extent_count && total < size; ++i) {
        struct appendfs_extent *ext = &inode->extents[i];
        off_t ext_end = ext->logical_offset + ext->length;
        if (offset >= ext_end) {
            continue;
        }
        off_t start = offset > ext->logical_offset ? offset : ext->logical_offset;
        off_t end = ext_end;
        if (end > offset + (off_t)(size - total)) {
            end = offset + (off_t)(size - total);
        }
        if (start >= end) {
            continue;
        }
        size_t read_len = (size_t)(end - start);
        off_t data_pos = ext->data_offset + (start - ext->logical_offset);
        if (pread(ctx->data_fd, out + total, read_len, data_pos) != (ssize_t)read_len) {
            return -1;
        }
        total += read_len;
    }
    if (total > 0) {
        inode->atime = time(NULL);
    }
    return (ssize_t)total;
}

int appendfs_stat(struct appendfs_context *ctx, const char *path, struct stat *st) {
    if (!ctx || !path || !st) {
        errno = EINVAL;
        return -1;
    }
    struct appendfs_inode *inode = find_inode_by_path(ctx, path);
    if (!inode || inode->deleted) {
        errno = ENOENT;
        return -1;
    }
    memset(st, 0, sizeof(*st));
    st->st_mode = inode->mode;
    st->st_size = inode->size;
    st->st_ctime = inode->ctime;
    st->st_mtime = inode->mtime;
    st->st_atime = inode->atime;
    st->st_nlink = 1;
    st->st_ino = inode->inode_id;
    return 0;
}

int appendfs_statfs(struct appendfs_context *ctx, struct statvfs *st) {
    if (!ctx || !st) {
        errno = EINVAL;
        return -1;
    }
    if (statvfs(ctx->root_path, st) == -1) {
        return -1;
    }
    return 0;
}
