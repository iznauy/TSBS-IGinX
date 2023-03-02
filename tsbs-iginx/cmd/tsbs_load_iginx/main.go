// bulk_load_iginx loads an Iginx daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
)

// Program option vars:
var (
	//session *client.Session
	doAbortOnExist bool
)

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatIginx)
	config = load.BenchmarkRunnerConfig{}
	// Not all the default flags apply to Iginx
	// config.AddToFlagSet(pflag.CommandLine)
	pflag.CommandLine.Uint("batch-size", 10, "Number of items to batch together in a single insert")
	pflag.CommandLine.Uint("workers", 1, "Number of parallel clients inserting")
	pflag.CommandLine.Int64("limit", 0, "Number of items to insert (0 = all of them).")
	pflag.CommandLine.Bool("do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	pflag.CommandLine.Bool("no-flow-control", false, "Whether to use flow control. Set this flag to false to load all data first.")
	pflag.CommandLine.Duration("reporting-period", 10*time.Second, "Period to report write stats")
	pflag.CommandLine.String("file", "/home/humanfy/tmp_data", "File name to read data from")
	pflag.CommandLine.Int64("seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	pflag.CommandLine.Uint64("channel-capacity", 100000, "Channel capacity")
	pflag.CommandLine.String("insert-intervals", "", "Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s between inserts, worker 2 and others wait 2s")
	pflag.CommandLine.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	//session = client.NewSession("127.0.0.1", "6888", "root", "root")
	//if err := session.Open(); err != nil {
	//	log.Fatal(err)
	//}

	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(os.Stdin)}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	loader.RunBenchmark(&benchmark{})
}
