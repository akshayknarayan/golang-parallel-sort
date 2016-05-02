package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
)

func main() {
	args := os.Args[1:]

	num_threads := 1
	sz := 1000
	for i, arg := range args {
		switch arg {
		case "-t":
			num, _ := strconv.ParseInt(args[i+1], 10, 32)
			num_threads = int(num)
		case "-n":
			nextArg := args[i+1]
			s, _ := strconv.ParseInt(nextArg[:len(nextArg)-1], 10, 32)
			switch nextArg[len(nextArg)-1] {
			case 'g':
				sz = int(s * 1e9)
			case 'm':
				sz = int(s * 1e6)
			case 'k':
				sz = int(s * 1e3)
			default:
				sz = int(s)
			}
		}
	}
	runtime.GOMAXPROCS(num_threads)

	fmt.Printf("%d numbers on %d threads\n", sz, num_threads)
	nums := genParallel(sz, num_threads)
	fmt.Println(sort.IntsAreSorted(nums))
	sorted := mysort(nums, num_threads)
	fmt.Println(sort.IntsAreSorted(sorted))
}

func gen(partitionSize int, gend chan []int) {
	res := make([]int, partitionSize)
	for i, _ := range res {
		res[i] = rand.Int()
	}
	gend <- res
}

func genParallel(n int, threads int) []int {
	partitionSize := n / threads
	gend := make(chan []int, threads)
	if threads == 1 {
		go gen(n, gend)
		return <-gend
	}

	nums := make([]int, 0, n)

	for i := 0; i < threads; i++ {
		go gen(partitionSize, gend)
	}

	for i := 0; i < threads; i++ {
		nums = append(nums, <-gend...)
	}

	return nums
}

func mysort(nums []int, threads int) []int {
	semaphore := make(chan bool, threads)
	for i := 0; i < threads; i++ {
		semaphore <- true
	}

	inp := make(chan int, len(nums))
	res := make(chan []int)
	for _, v := range nums {
		inp <- v
	}
	go sortParallel(inp, res, semaphore)
	close(inp)

	return <-res
}

func sortParallel(num chan int, res chan []int, semaphore chan bool) {
	<-semaphore
	nums := make([]int, 0)
	for n := range num {
		nums = append(nums, n)
	}

	if len(nums) < 4096 {
		sort.Ints(nums)
		semaphore <- true
		res <- nums
		return
	}

	smalls := make(chan int, len(nums))
	bigs := make(chan int, len(nums))

	smallResult := make(chan []int)
	bigResult := make(chan []int)

	go sortParallel(smalls, smallResult, semaphore)
	go sortParallel(bigs, bigResult, semaphore)

	pivot := nums[rand.Intn(len(nums))]
	for _, v := range nums {
		if v <= pivot {
			smalls <- v
		} else {
			bigs <- v
		}
	}

	semaphore <- true

	close(smalls)
	close(bigs)

	res <- append(<-smallResult, <-bigResult...)
}
