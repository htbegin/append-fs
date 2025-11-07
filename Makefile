CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -pedantic -g
CPPFLAGS ?= -Iinclude
LDFLAGS ?= 
FUSE_CFLAGS ?= $(shell pkg-config --cflags fuse3 2>/dev/null)
FUSE_LIBS ?= $(shell pkg-config --libs fuse3 2>/dev/null)
FUSE_AVAILABLE := $(strip $(FUSE_CFLAGS)$(FUSE_LIBS))

LIB_OBJS = src/appendfs.o src/crc32.o
EXAMPLE_OBJS = examples/prototype.o
FUSE_OBJS = src/fuse_main.o

ifeq ($(FUSE_AVAILABLE),)
ALL_TARGETS := prototype
else
ALL_TARGETS := prototype appendfsd
endif

.PHONY: all clean

all: $(ALL_TARGETS)

prototype: $(LIB_OBJS) $(EXAMPLE_OBJS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $^ $(LDFLAGS) -o $@

ifeq ($(FUSE_AVAILABLE),)
appendfsd:
	@echo 'fuse3 headers not found; skipping appendfsd build'
else
appendfsd: $(LIB_OBJS) $(FUSE_OBJS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUSE_CFLAGS) $^ $(LDFLAGS) $(FUSE_LIBS) -o $@

src/fuse_main.o: src/fuse_main.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUSE_CFLAGS) -c $< -o $@

endif

src/%.o: src/%.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

examples/%.o: examples/%.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	$(RM) $(LIB_OBJS) $(EXAMPLE_OBJS) $(FUSE_OBJS) prototype appendfsd
