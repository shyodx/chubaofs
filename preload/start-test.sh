#!/bin/bash

if [ x"$*" = x"" ]; then
	echo "need parameter: file path"
	exit
fi

LD_PRELOAD=/home/shengyong/go/src/github.com/chubaofs/chubaofs/build/bin/libcfspreload.so ./test/test $*

#LD_LIBRARY_PATH=/home/shengyong/go/src/github.com/chubaofs/chubaofs/build/bin/ test/cfswrite mytestfile 1000 3
