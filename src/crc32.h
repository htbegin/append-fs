#ifndef APPENDFS_CRC32_H
#define APPENDFS_CRC32_H

#include <stddef.h>
#include <stdint.h>

uint32_t appendfs_crc32(const void *data, size_t length);

#endif
