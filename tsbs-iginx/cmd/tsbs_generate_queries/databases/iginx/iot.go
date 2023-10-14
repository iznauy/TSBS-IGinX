package iginx

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	iotReadingsTable = "readings"
)

// IoT produces TimescaleDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	panicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	iginxql := fmt.Sprintf("SELECT last(longitude), last(latitude) FROM readings.%s.*.*.*.*",
		i.getTruckWhereString(nTrucks))

	humanLabel := "Iginx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	//iginxql := "SELECT last(longitude), last(latitude) FROM readings.*.*.*.*.*"
	iginxql := "select a.longitude as longitude, b.latitude as latitude, a.truck as truck from (select truck, last_value(value) as longitude from (select transposition(*) from (select latitude, longitude from readings.*.*.*.*.*)) group by truck, name having name = 'longitude') as a, (select truck, last_value(value) as latitude from (select transposition(*) from (select latitude, longitude from readings.*.*.*.*.*)) group by truck, name having name = 'latitude') as b where a.truck = b.truck"
	humanLabel := "Iginx last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	iginxql := fmt.Sprintf("select truck, last_value(value) as fuel from (select transposition(fuel_state) from diagnostics.*.%s.*.*.*) group by truck having last_value(value) < 0.1;",
		i.GetRandomFleet())

	humanLabel := "Iginx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	fleet := i.GetRandomFleet()
	iginxql := fmt.Sprintf("select a.curr_load as load, a.truck as truck, b.capacity as capacity from (select truck, last_value(value) as curr_load from (select transposition(*) from diagnostics.*.%s.*.*.*) group by name, truck having name = 'current_load') as a, (select truck, last_value(value) as capacity from (select transposition(*) from diagnostics.*.%s.*.*.*) group by name, truck having name = 'load_capacity') as b where a.truck = b.truck",
		fleet, fleet)

	humanLabel := "Iginx trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration * 144)

	trucks, _ := i.GetRandomTrucks(1)
	iginxql := fmt.Sprintf("SELECT * FROM readings.%s.*.*.*.* where time >=%d and time <= %d",
		formatName(trucks[0]), interval.Start().Unix()*1000, interval.End().Unix()*1000)

	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 1000 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

func formatName(name string) string {
	parts := strings.Split(name, "_")
	truck := parts[0]
	index, _ := strconv.Atoi(parts[1])
	return fmt.Sprintf("%s_%04d", truck, index)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.* GROUP [%d, %d] BY 10ms",
		i.GetRandomFleet(), interval.Start().Unix(), interval.End().Unix())

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.* GROUP [%d, %d] BY 10ms",
		i.GetRandomFleet(), interval.Start().Unix(), interval.End().Unix())

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	iginxql := fmt.Sprintf("select sum(fuel_consumption) from readings.*.*.*.*.* agg level = 2")

	humanLabel := "Iginx average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.*", i.GetRandomFleet())

	humanLabel := "Iginx average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	iginxql := fmt.Sprintf("SELECT AVG(velocity) FROM readings.*.%s.*", i.GetRandomFleet())

	humanLabel := "Iginx average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	iginxql := fmt.Sprintf("select avg(current_load) from diagnostics.*.*.*.*.* agg level=1,2")

	humanLabel := "Iginx average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	start := i.Interval.Start().Unix()
	end := i.Interval.End().Unix()
	iginxql := fmt.Sprintf(`SELECT AVG(status) FROM diagnostics.*.*.*.* GROUP [%d, %d] BY time(1d)`, start, end)

	humanLabel := "Iginx daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	// not all implemented limited by iginx sql grammar
	start := i.Interval.Start().Unix()
	end := i.Interval.End().Unix()
	iginxql := fmt.Sprintf(`SELECT AVG(status) FROM diagnostics.*.*.*.* GROUP [%d, %d] BY time(1d)`, start, end)

	humanLabel := "Iginx truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
