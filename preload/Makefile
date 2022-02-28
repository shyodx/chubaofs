target := libcfspreload.so

SRC := $(wildcard *.c)

CFLAGS += -Wall -Werror -fPIC -g
LDFLAGS += -pthread -ldl -lrt

$(target): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS)


TESTWRITE := test/test
CFSWRITE := test/cfswrite
MYWRITE := test/mywrite

$(TESTWRITE): test/test.c
	gcc -o $@ $^

$(MYWRITE): test/mywrite.c
	gcc -o $@ $^

$(CFSWRITE): test/cfswrite.c
	gcc -o $@ $^ -I../build/bin/ -L../build/bin/ -lcfs

all: $(target) $(TESTWRITE) $(MYWRITE) $(CFSWRITE)

install:
	install $(target) $(BINDIR)

clean:
	rm $(target)
