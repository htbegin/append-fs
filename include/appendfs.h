#ifndef APPENDFS_H
#define APPENDFS_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define APPENDFS_MAX_NAME 255
#define APPENDFS_DEFAULT_BUFFER (4 * 1024 * 1024)
#define APPENDFS_MIN_FLUSH (4 * 1024)

struct appendfs_context;
struct appendfs_file;
struct appendfs_inode_info;

struct appendfs_inode_info {
    uint64_t inode_id;
    mode_t mode;
    off_t size;
    time_t ctime;
    time_t mtime;
    time_t atime;
};

struct appendfs_options {
    size_t write_buffer_size;
};

int appendfs_open(const char *root_path, struct appendfs_context **out_ctx);
void appendfs_close(struct appendfs_context *ctx);

int appendfs_set_options(struct appendfs_context *ctx, const struct appendfs_options *opts);

int appendfs_mkdirs(struct appendfs_context *ctx, const char *path, mode_t mode);
int appendfs_mkdir(struct appendfs_context *ctx, const char *path, mode_t mode);
int appendfs_create_file(struct appendfs_context *ctx, const char *path, mode_t mode);
int appendfs_unlink(struct appendfs_context *ctx, const char *path);
int appendfs_rmdir(struct appendfs_context *ctx, const char *path);
int appendfs_rename(struct appendfs_context *ctx, const char *from_path, const char *to_path);
int appendfs_symlink(struct appendfs_context *ctx, const char *target, const char *linkpath, mode_t mode);
ssize_t appendfs_readlink(struct appendfs_context *ctx, const char *path, char *buf, size_t size);

struct appendfs_file *appendfs_open_file(struct appendfs_context *ctx, const char *path, int flags, mode_t mode);
ssize_t appendfs_write(struct appendfs_file *file, const void *buf, size_t size, off_t offset);
ssize_t appendfs_read(struct appendfs_context *ctx, const char *path, void *buf, size_t size, off_t offset);
int appendfs_truncate(struct appendfs_context *ctx, const char *path, off_t size);
int appendfs_flush(struct appendfs_file *file);
int appendfs_close_file(struct appendfs_file *file);
int appendfs_fsync(struct appendfs_file *file, int datasync);
int appendfs_fsyncdir(struct appendfs_context *ctx);
off_t appendfs_seek(struct appendfs_file *file, off_t offset, int whence);
int appendfs_set_times(struct appendfs_context *ctx, const char *path, const struct timespec times[2]);
int appendfs_statfs(struct appendfs_context *ctx, struct statvfs *st);
int appendfs_is_directory_empty(struct appendfs_context *ctx, const char *path);

int appendfs_setxattr(struct appendfs_context *ctx, const char *path, const char *name, const void *value, size_t size, int flags);
ssize_t appendfs_getxattr(struct appendfs_context *ctx, const char *path, const char *name, void *value, size_t size);
ssize_t appendfs_listxattr(struct appendfs_context *ctx, const char *path, char *list, size_t size);
int appendfs_removexattr(struct appendfs_context *ctx, const char *path, const char *name);

typedef int (*appendfs_dir_iter_cb)(const char *name, const struct appendfs_inode_info *info, void *user_data);
int appendfs_iterate_children(struct appendfs_context *ctx, const char *dir_path, appendfs_dir_iter_cb cb, void *user_data);

int appendfs_stat(struct appendfs_context *ctx, const char *path, struct stat *st);

#ifdef __cplusplus
}
#endif

#endif
