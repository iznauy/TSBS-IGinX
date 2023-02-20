package main

import (
	"fmt"
	"github.com/iznauy/IGinX-client-go/client_v2"
	"github.com/iznauy/IGinX-client-go/rpc"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// allows for testing
var (
	printFn              = fmt.Printf
	connectionStringList = strings.Split("172.16.17.21:6777,172.16.17.22:6777,172.16.17.23:6777,172.16.17.24:6777", ",")
)

type processor struct {
	session *client_v2.Session
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func shuffle(nums []int) []int {
	for i := len(nums); i > 0; i-- {
		last := i - 1
		idx := rand.Intn(i)
		nums[last], nums[idx] = nums[idx], nums[last]
	}
	return nums
}

func (p *processor) Init(_ int, _, _ bool) {
	connectionStrings := ""
	numbers := make([]int, 0, len(connectionStringList))
	for i := 0; i < len(connectionStringList); i++ {
		numbers = append(numbers, i)
	}
	numbers = shuffle(numbers)
	for i := 0; i < len(numbers); i++ {
		if i > 0 {
			connectionStrings += ","
		}
		connectionStrings += connectionStringList[numbers[i]]
	}

	settings, err := client_v2.NewSessionSettings(connectionStrings)
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	p.session = client_v2.NewSession(settings)
	if err := p.session.Open(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) Close(_ bool) {
	if err := p.session.Close(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) logWithTimeout(doneChan chan int, timeout time.Duration, sqlChan chan string) {
	for {
		var printSQL bool
		select {
		case <-doneChan:
			printSQL = false
		case <-time.After(timeout):
			printSQL = true
		}
		sql := <-sqlChan
		if printSQL {
			lines := strings.Split(sql, "\n")
			lines = lines[0 : len(lines)-1]

			var path []string
			var timestamp int64
			var values [][]interface{}
			var types []rpc.DataType
			i := 0
			for i = 0; i < len(lines); i++ {
				tmp := strings.Split(lines[i], " ")
				tmp[0] = "type=" + tmp[0]
				fir := strings.Split(tmp[0], ",")
				device := ""
				for j := 0; j < len(fir); j++ {
					kv := strings.Split(fir[j], "=")
					device += kv[1]
					device += "."
				}
				timestamp, _ = strconv.ParseInt(tmp[2], 10, 64)
				timestamp /= 1000000
				device = device[0 : len(device)-1]
				device = strings.Replace(device, "-", "_", -1)

				sec := strings.Split(tmp[1], ",")
				for j := 0; j < len(sec); j++ {
					kv := strings.Split(sec[j], "=")
					path = append(path, device+"."+kv[0])
					v, err := strconv.ParseFloat(kv[1], 32)
					if err != nil {
						log.Fatal(err)
					}
					values = append(values, []interface{}{v})
					types = append(types, rpc.DataType_DOUBLE)
				}
			}

			fmt.Println(path)
			fmt.Println(timestamp)
			fmt.Println(values)

			//fmt.Println("try insert again")
			//timestamps := []int64{timestamp}
			//err := p.session.InsertColumnRecords(path, timestamps, values, types)
			//if err != nil {
			//	log.Println(err)
			//	panic(err)
			//}
			//fmt.Println("try insert success")

			//<-doneChan
		}
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if !doLoad {
		return 0, 0
	}

	lines := strings.Split(batch.buf.String(), "\n")
	lines = lines[0 : len(lines)-1]

	var path []string
	var timestamp int64
	var values [][]interface{}
	var types []rpc.DataType
	var tagsList []map[string]string

	i := 0
	for i = 0; i < len(lines); i++ {
		var tag map[string]string
		tag = make(map[string]string)
		tmp := strings.Split(lines[i], " ")
		tmp[0] = "type=" + tmp[0]
		fir := strings.Split(tmp[0], ",")
		device := fir[0] + "."
		for j := 1; j < len(fir); j++ {
			kv := strings.Split(fir[j], "=")
			if kv[0] == "name" || kv[0] == "fleet" {
				device += kv[1]
				device += "."
			} else {
				tag[kv[0]] = strings.Replace(kv[1], ".", "_", -1)
				tag[kv[0]] = strings.Replace(tag[kv[0]], "-", "_", -1)
			}
		}

		timestamp, _ = strconv.ParseInt(tmp[2], 10, 64)
		timestamp /= 1000000
		device = device[5 : len(device)-1]
		device = strings.Replace(device, "-", "_", -1)

		sec := strings.Split(tmp[1], ",")
		for j := 0; j < len(sec); j++ {
			kv := strings.Split(sec[j], "=")
			onePath := device + "." + kv[0]
			onePath = strings.Replace(onePath, "-", "_", -1)
			if !in(onePath, path) {
				path = append(path, onePath)
				v, err := strconv.ParseFloat(kv[1], 32)
				if err != nil {
					log.Fatal(err)
				}
				values = append(values, []interface{}{v})
				types = append(types, rpc.DataType_DOUBLE)
				tagsList = append(tagsList, tag)
			}
		}
	}

	timestamps := []int64{timestamp}

	err := p.session.InsertColumnRecords(path, timestamps, values, types, tagsList)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func in(target string, strArray []string) bool {
	for _, element := range strArray {
		if target == element {
			return true
		}
	}
	return false
}
