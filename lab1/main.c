#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <string.h>
#include "crc64.h"

#define BLOCK_SIZE (8 * 1024 * 1024) // 8 MiB


double elapsed(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

uint64_t read_file_seq(int fd) {
    uint8_t *buffer = malloc(BLOCK_SIZE);
    if (!buffer) { perror("malloc"); exit(1); }

    uint64_t crc = 0;
    ssize_t bytes;
    lseek(fd, 0, SEEK_SET);
    while ((bytes = read(fd, buffer, BLOCK_SIZE)) > 0)
        crc = crc64_update(crc, buffer, bytes);

    free(buffer);
    return crc;
}

uint64_t read_file_rand(int fd, off_t size) {
    uint8_t *buffer = malloc(BLOCK_SIZE);
    if (!buffer) { perror("malloc"); exit(1); }

    uint64_t crc = 0;
    size_t blocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    for (size_t i = 0; i < blocks; ++i) {
        off_t offset = (i % 2 == 0) ? (i * BLOCK_SIZE)
                                    : (size - (i + 1) * BLOCK_SIZE);
        if (offset < 0) offset = 0;
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes = read(fd, buffer, BLOCK_SIZE);
        if (bytes > 0)
            crc = crc64_update(crc, buffer, bytes);
    }

    free(buffer);
    return crc;
}

uint64_t mmap_seq(int fd, off_t size) {
    uint8_t *data = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) { perror("mmap"); exit(1); }

    uint64_t crc = crc64_update(0, data, size);
    munmap(data, size);
    return crc;
}

uint64_t mmap_rand(int fd, off_t size) {
    uint8_t *data = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) { perror("mmap"); exit(1); }

    uint64_t crc = 0;
    size_t blocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    for (size_t i = 0; i < blocks; ++i) {
        size_t offset = (i % 2 == 0) ? (i * BLOCK_SIZE)
                                     : (size - (i + 1) * BLOCK_SIZE);
        if (offset >= (size_t)size) continue;
        size_t len = (offset + BLOCK_SIZE > (size_t)size)
                         ? (size - offset)
                         : BLOCK_SIZE;
        crc = crc64_update(crc, data + offset, len);
    }

    munmap(data, size);
    return crc;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <file path>\n", argv[0]);
        return 1;
    }

    const char *path = argv[1];
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }

    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); return 1; }
    off_t size = st.st_size;

    struct timespec start, end;
    double t;
    uint64_t crc_read, crc_mmap;

    // 1. read() sekwencyjnie
    clock_gettime(CLOCK_MONOTONIC, &start);
    crc_read = read_file_seq(fd);
    clock_gettime(CLOCK_MONOTONIC, &end);
    t = elapsed(start, end);
    printf("1. read() sekwencyjnie: %.6f s, CRC64 = %016llx\n", t, crc_read);

    // 2. mmap() sekwencyjnie
	clock_gettime(CLOCK_MONOTONIC, &start);
    crc_mmap = mmap_seq(fd, size);
    clock_gettime(CLOCK_MONOTONIC, &end);
    t = elapsed(start, end);
    printf("2. mmap() sekwencyjnie: %.6f s, CRC64 = %016llx\n", t, crc_mmap);
	


	// 3. read() losowo
    clock_gettime(CLOCK_MONOTONIC, &start);
    crc_read = read_file_rand(fd, size);
    clock_gettime(CLOCK_MONOTONIC, &end);
    t = elapsed(start, end);
    printf("3. read() losowo:      %.6f s, CRC64 = %016llx\n", t, crc_read);

    // 4. mmap() losowo
    clock_gettime(CLOCK_MONOTONIC, &start);
    crc_mmap = mmap_rand(fd, size);
    clock_gettime(CLOCK_MONOTONIC, &end);
    t = elapsed(start, end);
    printf("4. mmap() losowo:      %.6f s, CRC64 = %016llx\n", t, crc_mmap);

    close(fd);
    return 0;
}