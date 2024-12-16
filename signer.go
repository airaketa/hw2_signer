package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const th = 6

var md5Mutex sync.Mutex

func ExecutePipeline(jobs ...job) {
	var wg sync.WaitGroup
	in := make(chan interface{})

	for _, job := range jobs {
		wg.Add(1)

		out := make(chan interface{})
		go func(in, out chan interface{}) {
			defer wg.Done()
			defer close(out)

			job(in, out)
		}(in, out)
		in = out
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}
	for i := range in {
		wg.Add(1)
		go doCalcSingleHash(strconv.Itoa(i.(int)), out, wg)
	}

	wg.Wait()
}

func doCalcSingleHash(data string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	crc32Channel := make(chan string)
	go func() {
		crc32Channel <- DataSignerCrc32(data)
	}()
	md5Data := dataSignerMd5(data)
	crc32Md5Data := DataSignerCrc32(md5Data)
	crc32Data := <-crc32Channel

	out <- crc32Data + "~" + crc32Md5Data
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	for inItem := range in {
		wg.Add(1)
		go doCalcMultiHash(inItem.(string), out, &wg)
	}
	wg.Wait()
}

func doCalcMultiHash(data string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var crc32Wg sync.WaitGroup
	crc32Wg.Add(th)
	var result [th]string
	for i := 0; i < th; i++ {
		go func() {
			defer crc32Wg.Done()
			result[i] = DataSignerCrc32(strconv.Itoa(i) + data)
		}()
	}
	crc32Wg.Wait()
	out <- strings.Join(result[:], "")
}

func CombineResults(in, out chan interface{}) {
	var resultArray []string

	for i := range in {
		resultArray = append(resultArray, i.(string))
	}

	sort.Strings(resultArray)
	out <- strings.Join(resultArray, "_")
}

func dataSignerMd5(data string) string {
	md5Mutex.Lock()
	defer md5Mutex.Unlock()

	return DataSignerMd5(data)
}
