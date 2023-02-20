// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/iznauy/IGinX-client-go/client_v2"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Global vars:
var (
	runner               *query.BenchmarkRunner
	connectionStringList = strings.Split("172.16.17.21:6777,172.16.17.22:6777,172.16.17.23:6777,172.16.17.24:6777", ",")
)

// Parse args:
func init() {
	rand.Seed(time.Now().UTC().UnixNano())

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	runner = query.NewBenchmarkRunner(config)
}

func shuffle(nums []int) []int {
	for i := len(nums); i > 0; i-- {
		last := i - 1
		idx := rand.Intn(i)
		nums[last], nums[idx] = nums[idx], nums[last]
	}
	return nums
}

func main() {
	runner.Run(&query.IginxPool, newProcessor)
}

type processor struct {
	session *client_v2.Session
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(_ int) {
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

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.Iginx)
	lag, err := Do(hq, p.session)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func Do(q *query.Iginx, session *client_v2.Session) (lag float64, err error) {
	sql := string(q.SqlQuery)
	start := time.Now()
	// execute sql
	cursor, err := session.ExecuteQuery(sql, 100)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}

	if _, err := cursor.GetFields(); err != nil {
		fmt.Println(err)
		return 0, err
	}
	for {
		hasMore, err := cursor.HasMore()
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		if !hasMore {
			break
		}
		if _, err := cursor.NextRow(); err != nil {
			fmt.Println(err)
			return 0, err
		}
	}
	if err := cursor.Close(); err != nil {
		fmt.Println(err)
		return 0, err
	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	return lag, err
}
