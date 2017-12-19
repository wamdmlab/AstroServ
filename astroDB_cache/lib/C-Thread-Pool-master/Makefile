CFLAGS = -D_REENTRANT -Wall -pedantic -Isrc
LDLIBS = -lpthread

ifdef DEBUG
CFLAGS += -g
LDFLAGS += -g
endif

TARGETS = libthreadpool.so libthreadpool.a

all: $(TARGETS)

src/threadpool.o: thpool.c thpool.h

# Short-hand aliases
shared: libthreadpool.so
static: libthreadpool.a

libthreadpool.so: thpool.c thpool.h
	$(CC) -shared -fPIC ${CFLAGS} -o $@ $< ${LDLIBS}

src/libthreadpool.o: thpool.c thpool.h
	$(CC) -c -fPIC ${CFLAGS} -o $@ $<

libthreadpool.a: thpool.o
	ar rcs $@ $^

clean:
	rm -f $(TARGETS) *~ */*~ */*.o

