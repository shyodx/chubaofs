target := libcfsdirect.so

SRC := $(wildcard *.c)
OBJ := $(patsubst %.c,%.o, $(SRC))

LIBCFSDIR := /home/shengyong/repo/cfs/kernel-bypass/build/bin
#LIBCFSDIR := /home/shengyong/repo/cfs/kernel-bypass/docker/bin

CFLAGS += -fPIC -g -I$(LIBCFSDIR)
LDFLAGS += -pthread -L$(LIBCFSDIR) -lcfs -ldl

$(target): $(OBJ)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS)

$(OBJ): $(SRC)
	$(CC) $(CFLAGS) -c $^ $(LDFLAGS)

TESTWRITE := test/test
CFSWRITE := test/cfswrite
MYWRITE := test/mywrite
TESTFORK := test/test-fork

$(TESTWRITE): test/test.c
	gcc -o $@ $^

$(MYWRITE): test/mywrite.c
	gcc -o $@ $^

$(TESTFORK): test/test-fork.c
	gcc -o $@ $^

$(CFSWRITE): test/cfswrite.c
	gcc -o $@ $^ -I$(LIBCFSDIR) -L$(LIBCFSDIR) -lcfs

all: $(target) $(TESTWRITE) $(MYWRITE) $(CFSWRITE) $(TESTFORK)

clean:
	rm $(OBJ)
