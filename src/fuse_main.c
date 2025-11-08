#define _POSIX_C_SOURCE 200809L
#define FUSE_USE_VERSION 35

#include "appendfs.h"

#include <errno.h>
#include <fuse3/fuse.h>
#include <fuse3/fuse_opt.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct afs_config {
    char *store_path;
    size_t write_buffer;
};

struct afs_state {
    struct appendfs_context *ctx;
    struct afs_config config;
};

#define AFS_OPT_KEY(t, p, v) { t, offsetof(struct afs_config, p), v }

static struct fuse_opt afs_opts[] = {
    AFS_OPT_KEY("--store=%s", store_path, 0),
    AFS_OPT_KEY("store=%s", store_path, 0),
    AFS_OPT_KEY("--buffer=%zu", write_buffer, 0),
    AFS_OPT_KEY("buffer=%zu", write_buffer, 0),
    FUSE_OPT_END
};

static struct appendfs_context *afs_context(void) {
    struct fuse_context *fc = fuse_get_context();
    struct afs_state *state = (struct afs_state *)fc->private_data;
    return state->ctx;
}

static int afs_fill_stat(const char *path, struct stat *stbuf) {
    struct fuse_context *fc = fuse_get_context();
    memset(stbuf, 0, sizeof(*stbuf));
    stbuf->st_uid = fc->uid;
    stbuf->st_gid = fc->gid;
    if (strcmp(path, "/") == 0) {
        time_t now = time(NULL);
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_mtime = now;
        stbuf->st_atime = now;
        stbuf->st_ctime = now;
        stbuf->st_ino = 1;
        return 0;
    }
    if (appendfs_stat(afs_context(), path, stbuf) == -1) {
        return -1;
    }
    stbuf->st_uid = fc->uid;
    stbuf->st_gid = fc->gid;
    stbuf->st_nlink = S_ISDIR(stbuf->st_mode) ? 2 : 1;
    return 0;
}

static int afs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void)fi;
    if (afs_fill_stat(path, stbuf) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_access(const char *path, int mask) {
    struct stat st;
    if (strcmp(path, "/") == 0) {
        memset(&st, 0, sizeof(st));
        st.st_mode = S_IFDIR | 0755;
    } else {
        if (appendfs_stat(afs_context(), path, &st) == -1) {
            return -errno;
        }
    }
    mode_t mode = st.st_mode;
    if (mask & R_OK) {
        if (!(mode & S_IRUSR)) {
            return -EACCES;
        }
    }
    if (mask & W_OK) {
        if (!(mode & S_IWUSR)) {
            return -EACCES;
        }
    }
    if (mask & X_OK) {
        if (!(mode & S_IXUSR)) {
            return -EACCES;
        }
    }
    return 0;
}

static int afs_opendir(const char *path, struct fuse_file_info *fi) {
    (void)fi;
    if (strcmp(path, "/") == 0) {
        return 0;
    }
    struct stat st;
    if (appendfs_stat(afs_context(), path, &st) == -1) {
        return -errno;
    }
    if (!S_ISDIR(st.st_mode)) {
        return -ENOTDIR;
    }
    return 0;
}

struct readdir_ctx {
    void *buf;
    fuse_fill_dir_t filler;
    enum fuse_fill_dir_flags fill_flags;
    uid_t uid;
    gid_t gid;
};

static int afs_readdir_cb(const char *name, const struct appendfs_inode_info *info, void *user_data) {
    struct readdir_ctx *ctx = (struct readdir_ctx *)user_data;
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = info->mode;
    st.st_nlink = S_ISDIR(info->mode) ? 2 : 1;
    st.st_size = info->size;
    st.st_ctime = info->ctime;
    st.st_mtime = info->mtime;
    st.st_atime = info->atime;
    st.st_uid = ctx->uid;
    st.st_gid = ctx->gid;
    st.st_ino = info->inode_id;
    if (ctx->filler(ctx->buf, name, &st, 0, ctx->fill_flags) != 0) {
        return 1;
    }
    return 0;
}

static int afs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                       struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    (void)offset;
    (void)fi;
    struct fuse_context *fc = fuse_get_context();
    enum fuse_fill_dir_flags fill_flags = 0;
#ifdef FUSE_READDIR_PLUS
    if (flags & FUSE_READDIR_PLUS) {
#ifdef FUSE_FILL_DIR_PLUS
        fill_flags |= FUSE_FILL_DIR_PLUS;
#endif
    }
#endif
    struct readdir_ctx ctx = {
        .buf = buf,
        .filler = filler,
        .fill_flags = fill_flags,
        .uid = fc->uid,
        .gid = fc->gid,
    };
    struct stat current;
    if (afs_fill_stat(path, &current) == -1) {
        return -errno;
    }
    filler(buf, ".", &current, 0, fill_flags);

    struct stat parent = current;
    if (strcmp(path, "/") != 0) {
        char *tmp = strdup(path);
        if (!tmp) {
            return -ENOMEM;
        }
        char *slash = strrchr(tmp, '/');
        const char *parent_path = "/";
        if (slash && slash != tmp) {
            *slash = '\0';
            parent_path = tmp;
        }
        if (afs_fill_stat(parent_path, &parent) == -1) {
            free(tmp);
            return -errno;
        }
        free(tmp);
    }
    filler(buf, "..", &parent, 0, fill_flags);
    if (appendfs_iterate_children(afs_context(), path, afs_readdir_cb, &ctx) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_releasedir(const char *path, struct fuse_file_info *fi) {
    (void)path;
    (void)fi;
    return 0;
}

static int afs_mkdir(const char *path, mode_t mode) {
    if (appendfs_mkdir(afs_context(), path, mode) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_unlink(const char *path) {
    if (appendfs_unlink(afs_context(), path) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_rmdir(const char *path) {
    if (appendfs_rmdir(afs_context(), path) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_rename(const char *from, const char *to, unsigned int flags) {
    if (flags != 0) {
        return -EOPNOTSUPP;
    }
    if (appendfs_rename(afs_context(), from, to) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_link(const char *from, const char *to) {
    (void)from;
    (void)to;
    return -EOPNOTSUPP;
}

static int afs_symlink(const char *from, const char *to) {
    if (appendfs_symlink(afs_context(), from, to, 0777) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_readlink(const char *path, char *buf, size_t size) {
    ssize_t rc = appendfs_readlink(afs_context(), path, buf, size);
    if (rc < 0) {
        return -errno;
    }
    return 0;
}

static struct appendfs_file *afs_file_from_fi(struct fuse_file_info *fi) {
    return (struct appendfs_file *)(uintptr_t)fi->fh;
}

static int afs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    struct appendfs_file *file = appendfs_open_file(afs_context(), path, fi->flags, mode);
    if (!file) {
        return -errno;
    }
    fi->fh = (uint64_t)(uintptr_t)file;
    return 0;
}

static int afs_open(const char *path, struct fuse_file_info *fi) {
    int flags = fi->flags & ~(O_CREAT | O_EXCL);
    struct appendfs_file *file = appendfs_open_file(afs_context(), path, flags, 0);
    if (!file) {
        return -errno;
    }
    fi->fh = (uint64_t)(uintptr_t)file;
    return 0;
}

static int afs_flush(const char *path, struct fuse_file_info *fi) {
    (void)path;
    struct appendfs_file *file = afs_file_from_fi(fi);
    if (!file) {
        return -EBADF;
    }
    if (appendfs_flush(file) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_release(const char *path, struct fuse_file_info *fi) {
    (void)path;
    struct appendfs_file *file = afs_file_from_fi(fi);
    if (!file) {
        return -EBADF;
    }
    if (appendfs_close_file(file) == -1) {
        return -errno;
    }
    fi->fh = 0;
    return 0;
}

static int afs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void)fi;
    if (strcmp(path, "/") == 0) {
        return 0;
    }
    ssize_t rc = appendfs_read(afs_context(), path, buf, size, offset);
    if (rc < 0) {
        return -errno;
    }
    return (int)rc;
}

static int afs_write_buf(const char *path, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *fi) {
    (void)path;
    struct appendfs_file *file = afs_file_from_fi(fi);
    if (!file) {
        return -EBADF;
    }
    size_t len = fuse_buf_size(buf);
    if (len == 0) {
        return 0;
    }
    struct fuse_bufvec dst = FUSE_BUFVEC_INIT(len);
    unsigned char *tmp = NULL;
    if (len > 0) {
        tmp = malloc(len);
        if (!tmp) {
            return -ENOMEM;
        }
        dst.buf[0].mem = tmp;
        ssize_t copied = fuse_buf_copy(&dst, buf, 0);
        if (copied < 0 || (size_t)copied != len) {
            free(tmp);
            return -EIO;
        }
    }
    ssize_t written = appendfs_write(file, tmp, len, off);
    free(tmp);
    if (written < 0) {
        return -errno;
    }
    return (int)written;
}

static int afs_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    (void)fi;
    if (strcmp(path, "/") == 0) {
        return -EISDIR;
    }
    if (appendfs_truncate(afs_context(), path, size) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    (void)path;
    struct appendfs_file *file = afs_file_from_fi(fi);
    if (!file) {
        return -EBADF;
    }
    if (appendfs_fsync(file, datasync) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) {
    (void)path;
    (void)datasync;
    (void)fi;
    if (appendfs_fsyncdir(afs_context()) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_statfs(const char *path, struct statvfs *st) {
    (void)path;
    if (appendfs_statfs(afs_context(), st) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
    (void)fi;
    if (strcmp(path, "/") == 0) {
        return 0;
    }
    if (appendfs_set_times(afs_context(), path, tv) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
    if (appendfs_setxattr(afs_context(), path, name, value, size, flags) == -1) {
        return -errno;
    }
    return 0;
}

static int afs_getxattr(const char *path, const char *name, char *value, size_t size) {
    ssize_t rc = appendfs_getxattr(afs_context(), path, name, value, size);
    if (rc < 0) {
        return -errno;
    }
    return (int)rc;
}

static int afs_listxattr(const char *path, char *list, size_t size) {
    ssize_t rc = appendfs_listxattr(afs_context(), path, list, size);
    if (rc < 0) {
        return -errno;
    }
    return (int)rc;
}

static int afs_removexattr(const char *path, const char *name) {
    if (appendfs_removexattr(afs_context(), path, name) == -1) {
        return -errno;
    }
    return 0;
}

static off_t afs_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi) {
    (void)path;
    struct appendfs_file *file = afs_file_from_fi(fi);
    if (!file) {
        errno = EBADF;
        return (off_t)-1;
    }
    return appendfs_seek(file, off, whence);
}

static const struct fuse_operations afs_oper = {
    .getattr = afs_getattr,
    .access = afs_access,
    .opendir = afs_opendir,
    .readdir = afs_readdir,
    .releasedir = afs_releasedir,
    .mkdir = afs_mkdir,
    .rmdir = afs_rmdir,
    .unlink = afs_unlink,
    .rename = afs_rename,
    .link = afs_link,
    .symlink = afs_symlink,
    .readlink = afs_readlink,
    .create = afs_create,
    .open = afs_open,
    .release = afs_release,
    .flush = afs_flush,
    .read = afs_read,
    .write_buf = afs_write_buf,
    .truncate = afs_truncate,
    .fsync = afs_fsync,
    .fsyncdir = afs_fsyncdir,
    .statfs = afs_statfs,
    .utimens = afs_utimens,
    .setxattr = afs_setxattr,
    .getxattr = afs_getxattr,
    .listxattr = afs_listxattr,
    .removexattr = afs_removexattr,
    .lseek = afs_lseek,
};

int main(int argc, char *argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct afs_state state;
    memset(&state, 0, sizeof(state));
    state.config.write_buffer = APPENDFS_DEFAULT_BUFFER;

    if (fuse_opt_parse(&args, &state.config, afs_opts, NULL) == -1) {
        fprintf(stderr, "appendfs: failed to parse options\n");
        return 1;
    }
    if (!state.config.store_path) {
        fprintf(stderr, "appendfs: --store=<path> option is required\n");
        fuse_opt_free_args(&args);
        return 1;
    }
    char *store_copy = strdup(state.config.store_path);
    if (!store_copy) {
        fprintf(stderr, "appendfs: out of memory\n");
        fuse_opt_free_args(&args);
        return 1;
    }
    if (appendfs_open(store_copy, &state.ctx) == -1) {
        fprintf(stderr, "appendfs: failed to open store %s: %s\n", store_copy, strerror(errno));
        free(store_copy);
        fuse_opt_free_args(&args);
        return 1;
    }
    free(store_copy);

    if (state.config.write_buffer && state.config.write_buffer != APPENDFS_DEFAULT_BUFFER) {
        struct appendfs_options opts = {
            .write_buffer_size = state.config.write_buffer,
        };
        if (appendfs_set_options(state.ctx, &opts) == -1) {
            fprintf(stderr, "appendfs: invalid buffer size\n");
            appendfs_close(state.ctx);
            fuse_opt_free_args(&args);
            return 1;
        }
    }

    int ret = fuse_main(args.argc, args.argv, &afs_oper, &state);
    appendfs_close(state.ctx);
    fuse_opt_free_args(&args);
    return ret;
}
