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
	connectionStringList = strings.Split("172.40.0.52:6888,172.40.0.53:6888,172.40.0.54:6888,172.40.0.55:6888", ",") //connectionStringList = []string{"127.0.0.1:6888"}

	defaultTruck = "unknown"
	defaultTagK  = []string{"fleet", "driver", "model", "device_version"}
	defaultTagV  = []string{"unknown", "unknown", "unknown", "unknown"}
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

func formatName(name string) string {
	parts := strings.Split(name, "_")
	truck := parts[0]
	index, _ := strconv.Atoi(parts[1])
	return fmt.Sprintf("%s_%04d", truck, index)
}

func parseMeasurementAndValues(measurement string, fields string) ([]string, []float64) {
	var paths []string
	var values []float64

	fir := strings.Split(measurement, ",")
	device := fir[0] + "."
	if !strings.Contains(fir[1], "truck") {
		device += defaultTruck
		device += "."
		fir = fir[1:]
	} else {
		device += formatName(strings.Split(fir[1], "=")[1])
		device += "."
		fir = fir[2:]
	}

	index := 0
	for j := 0; j < len(defaultTagK); j++ {
		if index < len(fir) {
			kv := strings.Split(fir[index], "=")
			if defaultTagK[j] == kv[0] {
				device += strings.Replace(kv[1], ".", "_", -1)
				device += "."
				index++
				continue
			}
		}
		device += defaultTagV[j]
		device += "."
	}
	device = strings.Replace(device, "-", "_", -1)

	sec := strings.Split(fields, ",")
	for j := 0; j < len(sec); j++ {
		kv := strings.Split(sec[j], "=")
		path := device + kv[0]
		path = strings.Replace(path, "-", "_", -1)

		v, err := strconv.ParseFloat(kv[1], 32)
		if err != nil {
			log.Fatal(err)
		}
		paths = append(paths, path)
		values = append(values, v)
	}
	return paths, values
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	beginTime := time.Now().UnixMilli()
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if !doLoad {
		return 0, 0
	}

	lines := strings.Split(batch.buf.String(), "\n")
	lines = lines[0 : len(lines)-1]

	var paths []string
	var timestamps []int64
	var timestampIndices = make(map[int64]int)
	var values [][]interface{}
	var types []rpc.DataType
	var pathIndices = make(map[string]int)

	for _, line := range lines {
		parts := strings.Split(line, " ")
		subPaths, _ := parseMeasurementAndValues(parts[0], parts[1])
		for _, subPath := range subPaths {
			if _, ok := pathIndices[subPath]; ok {
				continue
			}
			pathIndices[subPath] = len(paths)
			paths = append(paths, subPath)
			types = append(types, rpc.DataType_DOUBLE)
		}
		timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
		if _, ok := timestampIndices[timestamp]; !ok {
			timestampIndices[timestamp] = len(timestamps)
			timestamps = append(timestamps, timestamp)
		}
	}

	for range paths {
		values = append(values, make([]interface{}, len(timestamps), len(timestamps)))
	}

	for _, line := range lines {
		secondIndex := 0
		parts := strings.Split(line, " ")
		timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
		for i := range timestamps {
			if timestamps[i] == timestamp {
				secondIndex = i
				break
			}
		}
		subPaths, subValues := parseMeasurementAndValues(parts[0], parts[1])
		for i, subPath := range subPaths {
			firstIndex := pathIndices[subPath]
			values[firstIndex][secondIndex] = subValues[i]
		}
	}

	var err error
	for i := 0; i < 3; i++ {
		err = p.session.InsertNonAlignedColumnRecords(paths, timestamps, values, types, nil)
		if err == nil {
			break
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	span := time.Now().UnixMilli() - beginTime
	if err != nil {
		log.Printf("[write stats] Span = %dms, Failure: %v\n", span, err)
		return 0, 0
	}
	log.Printf("[write stats] Span = %dms, Success\n", span)
	return metricCnt, uint64(rowCnt)
}
