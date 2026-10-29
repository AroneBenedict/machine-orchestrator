CC     ?= gcc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Werror -D_POSIX_C_SOURCE=200809L -Iinclude
LDLIBS  = -pthread
SRC     = src/mq.c src/machine.c

.PHONY: all test tsan asan clean
all: build/orchsim build/tests

build:
	mkdir -p build
build/orchsim: $(SRC) src/orchsim.c | build
	$(CC) $(CFLAGS) -pthread -o $@ $^ $(LDLIBS)
build/tests: $(SRC) tests/test_main.c | build
	$(CC) $(CFLAGS) -pthread -o $@ $^ $(LDLIBS)

test: build/tests
	./build/tests

# ThreadSanitizer: detects data races and lock-order inversions
tsan: | build
	$(CC) -std=c11 -g -O1 -D_POSIX_C_SOURCE=200809L -Iinclude -fsanitize=thread -pthread \
	    -o build/tests_tsan $(SRC) tests/test_main.c
	./build/tests_tsan

asan: | build
	$(CC) -std=c11 -g -O1 -D_POSIX_C_SOURCE=200809L -Iinclude -fsanitize=address,undefined \
	    -pthread -o build/tests_asan $(SRC) tests/test_main.c
	./build/tests_asan

clean:
	rm -rf build
