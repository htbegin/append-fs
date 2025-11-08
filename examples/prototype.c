#include "appendfs.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void hexdump(const unsigned char *data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        printf("%02x", data[i]);
        if ((i + 1) % 32 == 0) {
            putchar('\n');
        } else if ((i + 1) % 2 == 0) {
            putchar(' ');
        }
    }
    if (len % 32 != 0) {
        putchar('\n');
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <root>\n", argv[0]);
        return 1;
    }

    struct appendfs_context *ctx = NULL;
    if (appendfs_open(argv[1], &ctx) == -1) {
        fprintf(stderr, "appendfs_open failed: %s\n", strerror(errno));
        return 1;
    }

    const char *path = "demo/file.bin";
    if (appendfs_mkdirs(ctx, "demo", 0755) == -1) {
        fprintf(stderr, "mkdirs failed: %s\n", strerror(errno));
        appendfs_close(ctx);
        return 1;
    }

    if (appendfs_create_file(ctx, path, 0644) == -1 && errno != EEXIST) {
        fprintf(stderr, "create failed: %s\n", strerror(errno));
        appendfs_close(ctx);
        return 1;
    }

    struct appendfs_file *file = appendfs_open_file(ctx, path, O_CREAT | O_RDWR, 0644);
    if (!file) {
        fprintf(stderr, "open file failed: %s\n", strerror(errno));
        appendfs_close(ctx);
        return 1;
    }

    size_t payload_size = (4 * 1024 * 1024) + 8192;
    unsigned char *payload = malloc(payload_size);
    if (!payload) {
        fprintf(stderr, "out of memory\n");
        appendfs_close_file(file);
        appendfs_close(ctx);
        return 1;
    }
    for (size_t i = 0; i < payload_size; ++i) {
        payload[i] = (unsigned char)(i & 0xffu);
    }

    if (appendfs_write(file, payload, payload_size, 0) < 0) {
        fprintf(stderr, "write failed: %s\n", strerror(errno));
        free(payload);
        appendfs_close_file(file);
        appendfs_close(ctx);
        return 1;
    }

    if (appendfs_flush(file) == -1) {
        fprintf(stderr, "flush failed: %s\n", strerror(errno));
        free(payload);
        appendfs_close_file(file);
        appendfs_close(ctx);
        return 1;
    }

    if (appendfs_close_file(file) == -1) {
        fprintf(stderr, "close file failed: %s\n", strerror(errno));
        free(payload);
        appendfs_close(ctx);
        return 1;
    }

    unsigned char readback[64];
    ssize_t bytes = appendfs_read(ctx, path, readback, sizeof(readback), payload_size - sizeof(readback));
    if (bytes < 0) {
        fprintf(stderr, "read failed: %s\n", strerror(errno));
        free(payload);
        appendfs_close(ctx);
        return 1;
    }

    struct stat st;
    if (appendfs_stat(ctx, path, &st) == -1) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        free(payload);
        appendfs_close(ctx);
        return 1;
    }

    printf("file size: %lld\n", (long long)st.st_size);
    printf("tail bytes (%zd):\n", bytes);
    hexdump(readback, (size_t)bytes);

    free(payload);
    appendfs_close(ctx);
    return 0;
}
