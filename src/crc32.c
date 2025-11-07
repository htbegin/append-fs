#include "crc32.h"

static uint32_t crc_table[256];
static int crc_table_initialized = 0;

static void appendfs_crc32_init(void) {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) {
            if (c & 1) {
                c = 0xedb88320u ^ (c >> 1);
            } else {
                c >>= 1;
            }
        }
        crc_table[i] = c;
    }
    crc_table_initialized = 1;
}

uint32_t appendfs_crc32(const void *data, size_t length) {
    if (!crc_table_initialized) {
        appendfs_crc32_init();
    }

    const unsigned char *buf = (const unsigned char *)data;
    uint32_t c = 0xffffffffu;

    for (size_t i = 0; i < length; ++i) {
        c = crc_table[(c ^ buf[i]) & 0xffu] ^ (c >> 8);
    }

    return c ^ 0xffffffffu;
}
