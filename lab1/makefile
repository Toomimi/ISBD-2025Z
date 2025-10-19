CC = gcc
CFLAGS = -O2 -Wall -Wextra -std=c11

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LDLIBS += -lrt
endif

SRC = main.c crc64.c
OBJ = $(SRC:.c=.o)
TARGET = file_benchmark

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) 
%.o: %.c crc64.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)