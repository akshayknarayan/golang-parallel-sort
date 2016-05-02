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
	mysort(nums)
	fmt.Println(sort.IntsAreSorted(nums))
}

func gen(part []int, done chan int) {
	for i, _ := range part {
		part[i] = rand.Int()
	}
	done <- 0
}

func genParallel(n int, threads int) []int {
	nums := make([]int, n)
	partitionSize := n / threads

	waiters := make(chan int, threads)
	for i := 0; i < threads; i++ {
		idx := i * partitionSize
		go gen(nums[idx:idx+partitionSize], waiters)
	}

	for i := 0; i < threads; i++ {
		<-waiters
	}

	return nums
}

func merge(part []int) {
	// left and right halves are sorted
	// merge the two halves
	merged := make([]int, 0, len(part))
	i := 0
	j := len(part) / 2
	for i < len(part)/2 && j < len(part) {
		if part[i] <= part[j] {
			merged = append(merged, part[i])
			i += 1
		} else {
			merged = append(merged, part[j])
			j += 1
		}
	}

	for i < len(part)/2 {
		merged = append(merged, part[i])
		i += 1
	}
	for j < len(part) {
		merged = append(merged, part[j])
		j += 1
	}

	for i, _ := range part {
		part[i] = merged[i]
	}
}

func sortPart(part []int, done chan int) {
	if len(part) < 65536 {
		sort.Ints(part)
		done <- 0
		return
	}

	halfway := len(part) / 2
	leftDone := make(chan int)
	rightDone := make(chan int)
	go sortPart(part[:halfway], leftDone)
	go sortPart(part[halfway:], rightDone)
	<-leftDone
	<-rightDone
	merge(part)
	done <- 0
}

func mysort(nums []int) {
	done := make(chan int)
	go sortPart(nums, done)
	<-done
}
