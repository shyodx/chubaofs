target := libcfspreload.so

SRC := $(wildcard *.c)

CFLAGS += -Wall -Werror -fPIC -g
LDFLAGS += -pthread -ldl -lrt

$(target): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS)

all: $(target)

install:
	install $(target) $(BINDIR)

clean:
	rm $(target)
