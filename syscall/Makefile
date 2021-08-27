target := libcfsdirect.so

SRC := $(wildcard *.c)
OBJ := $(patsubst %.c,%.o, $(SRC))

LIBCFSDIR := /home/shengyong/go/src/github.com/chubaofs/chubaofs/build/bin

CFLAGS += -fPIC -g -I$(LIBCFSDIR)
LDFLAGS += -pthread -L$(LIBCFSDIR) -lcfs -ldl

$(target): $(OBJ)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS)

$(OBJ): $(SRC)
	$(CC) $(CFLAGS) -c $^ $(LDFLAGS)

clean:
	rm $(OBJ)
