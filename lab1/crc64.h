#ifndef CRC64_H
#define CRC64_H

#include <stdint.h>
#include <stddef.h>

uint64_t crc64_update(uint64_t crc, const void *data, size_t len);

#endif // CRC64_H