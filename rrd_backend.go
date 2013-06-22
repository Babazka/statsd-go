package main

import (
	"os"
	"./rrd"
	"log"
	"time"
)

const (
    RRD_STEP = 30
)

type RrdBackend struct {
}

func NewRrdBackend() RrdBackend {
    var b RrdBackend;
    return b;
}

func (b RrdBackend) beginAggregation() {
    ensure_rrd_dir_exists()
}
func (b RrdBackend) endAggregation() {
}
func (b RrdBackend) handleCounter(name string, count int64, count_ps float64) {
    write_to_gauge_rrd(name, count)
}
func (b RrdBackend) handleGauge(name string, count int64) {
    write_to_gauge_rrd(name, count)
}
func (b RrdBackend) handleTiming(name string, td TimerDistribution) {
    write_to_timing_rrd(name, td.min, td.max, td.mean, td.q_50, td.q_90, td.count);
}

func ensure_rrd_dir_exists() {
    if _, err := os.Stat(RRD_DIR); err == nil {
        return
    }
    os.Mkdir(RRD_DIR, 0755)
}

func mk_common_rrd(filename string) *rrd.Creator {
    t := time.Unix(time.Now().Unix() - (RRD_STEP), 0)
    c := rrd.NewCreator(filename, t, (RRD_STEP))
    c.RRA("AVERAGE", 0.5, 1,         4 * 60 * 60 / (RRD_STEP))
    c.RRA("AVERAGE", 0.5, 5,         3 * 24 * 60 * 60 / (5 * (RRD_STEP)))
    c.RRA("AVERAGE", 0.5, 30,       31 * 24 * 60 * 60 / (30 * (RRD_STEP)))
    c.RRA("AVERAGE", 0.5, 6 * 60,    3 * 31 * 24 * 60 * 60 / (6 * 60 * (RRD_STEP)))
    c.RRA("AVERAGE", 0.5, 24 * 60, 365 * 24 * 60 * 60 / (24 * 60 * (RRD_STEP)))
    return c
}

func mk_metric_filename(metric string) string {
    return RRD_DIR + "/" + metric + ".rrd"
}

func ensure_gauge_rrd_exists(metric string) {
    filename := mk_metric_filename(metric)
    if _, err := os.Stat(filename); err == nil {
        return
    }
    if *debug {
        log.Println("Creating rrd %s\n", filename)
    }
    c := mk_common_rrd(filename)
    c.DS("num", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    err := c.Create(true)
    if err != nil {
        panic("could not create rrd file: " + err.Error())
    }
}

func write_to_gauge_rrd(metric string, value int64) {
    metric = metric + ".gauge"
    filename := mk_metric_filename(metric)
    ensure_gauge_rrd_exists(metric)
    u := rrd.NewUpdater(filename)

    err := u.Update(time.Now(), value)
    if err != nil {
        panic("could not update gauge rrd file: " + err.Error())
    }
}

func ensure_timing_rrd_exists(metric string) {
    filename := mk_metric_filename(metric)
    if _, err := os.Stat(filename); err == nil {
        return
    }
    if *debug {
        log.Println("Creating dist rrd %s\n", filename)
    }
    c := mk_common_rrd(filename)
    c.DS("min", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    c.DS("max", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    c.DS("avg", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    c.DS("med", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    c.DS("q90", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    c.DS("num", "GAUGE", 2 * (RRD_STEP), 0, 2147483647)
    err := c.Create(true)
    if err != nil {
        panic("could not create dist-rrd file: " + err.Error())
    }
}

func write_to_timing_rrd(metric string, min float64, max float64, avg float64,
                         med float64, q90 float64, num int) {
    metric = metric + ".timing"
    filename := mk_metric_filename(metric)
    ensure_timing_rrd_exists(metric)
    u := rrd.NewUpdater(filename)

    err := u.Update(time.Now(), min, max, avg, med, q90, num)
    if err != nil {
        panic("could not update dist-rrd file: " + err.Error())
    }
}
