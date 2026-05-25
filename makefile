CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -pthread

.PHONY: all clean

all: procx

procx: procx.c
	$(CC) $(CFLAGS) -o procx procx.c

clean:
	rm -f procx
