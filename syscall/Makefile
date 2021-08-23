target := libcfsdirect.so

SRC := $(wildcard *.c)
OBJ := $(patsubst %.c,%.o, $(SRC))

CFLAGS += -fPIC -g
LDFLAGS += -pthread

$(target): $(OBJ)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS)

$(OBJ): $(SRC)
	$(CC) $(CFLAGS) -c $^ $(LDFLAGS)

clean:
	rm $(OBJ)
