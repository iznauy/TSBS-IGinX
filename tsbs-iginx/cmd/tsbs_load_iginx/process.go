package main

import (
	"fmt"
	"github.com/thulab/iginx-client-go/client"
	"github.com/thulab/iginx-client-go/rpc"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"strconv"
	"strings"
	"time"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	session *client.Session
}

func (p *processor) Init(numWorker int, _, _ bool) {
	//if numWorker%2 == 0 {
	//	p.session = client.NewSession("172.16.17.21", "6888", "root", "root")
	//} else if numWorker%2 == 1 {
	//	p.session = client.NewSession("172.16.17.23", "6888", "root", "root")
	//}
	p.session = client.NewSession("172.16.17.21", "6880", "root", "root")
	if err := p.session.Open(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) Close(_ bool) {
	if err := p.session.Close(); err != nil {
		log.Fatal(err)
	}
}

func run(done chan int, startTime int64, lines *[]string) {
	for {
		select {
		case <-done:
			done <- 1
			break
		default:
		}

		if (time.Now().Unix() - startTime) > 10000 {
			log.Println(lines)
			break
		}
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

	i := 0
	for i = 0; i < len(lines); i++ {
		tmp := strings.Split(lines[i], " ")
		tmp[0] = "type=" + tmp[0]
		fir := strings.Split(tmp[0], ",")
		device := fir[0] + "."
		for j := 1; j < len(fir); j++ {
			kv := strings.Split(fir[j], "=")
			device += strings.Replace(kv[1], ".", "_", -1)
			device += "."
			device = strings.Replace(device, "-", "_", -1)
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
			}
		}
	}

	timestamps := []int64{timestamp}

	//c := make(chan int, 1000)
	//sqlChan := make(chan string, 1000)
	//sqlChan <- batch.buf.String()
	//go p.logWithTimeout(c, 10*time.Second, sqlChan)

	err := p.session.InsertColumnRecords(path, timestamps, values, types, nil)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	//c <- 1

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
