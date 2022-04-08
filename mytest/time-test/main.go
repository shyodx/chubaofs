package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/metanode"
)

var loop int = 1000000

func main() {
	var (
		t   time.Time
		err error
	)

	if len(os.Args) == 2 {
		loop, err = strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Printf("Failed to atoi parameter 1 (%s)\n", os.Args[1])
			return
		}
	}

	start := time.Now()
	for i := 0; i < loop; i++ {
		t = time.Now()
	}
	end := time.Now()
	fmt.Printf("Loop[%v] time.Now() cost %v last(%v)\n\n", loop, end.Sub(start), t)

	start = time.Now()
	for i := 0; i < loop; i++ {
		t = metanode.Now.GetCurrentTime()
	}
	end = time.Now()
	fmt.Printf("Loop[%v] metanode.Now.GetCurrentTime() cost %v last(%v)\n", loop, end.Sub(start), t)
}
