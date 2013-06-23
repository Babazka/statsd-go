// +build !rrd

package main

import (
	"os"
	"log"
	"path/filepath"
	"strings"
	"./whisper"
	"time"
	"flag"
)

const (
    RRD_STEP = 30
)

var (
	whisperPath   = flag.String("whisper-data-dir", "data", "Path to storage directory for whisper files")
	archiveParams  = flag.String("archive-params", "10s:3h,1m:7d,30m:1y", "Whisper archive params (pairs of precision:retention)")
    parsedArchiveParams whisper.Retentions
)

type RrdBackend struct {
}

func NewRrdBackend() *RrdBackend {
    var b RrdBackend;
    var err error;
    parsedArchiveParams, err = whisper.ParseRetentionDefs(*archiveParams)
    if err != nil {
        log.Fatalf("can't parse retention defs: %s", err)
    }
    log.Printf("Writing to Whisper files at %s/\n", *whisperPath)
    return &b;
}

func (b *RrdBackend) beginAggregation() {
}
func (b *RrdBackend) endAggregation() {
}
func (b *RrdBackend) handleCounter(name string, count int64, count_ps float64) {
    update_whisper_file("stats." + name, whisper.Average,
        count_ps, uint32(time.Now().Unix()))
}
func (b *RrdBackend) handleGauge(name string, count int64) {
    update_whisper_file("stats.gauges." + name, whisper.Average,
        float64(count), uint32(time.Now().Unix()))
}
func (b *RrdBackend) handleTiming(name string, td TimerDistribution) {
    t := uint32(time.Now().Unix())
    update_whisper_file("stats.timers." + name + ".count", whisper.Average, float64(td.count), t)
    update_whisper_file("stats.timers." + name + ".count_ps", whisper.Average, float64(td.count_ps), t)
    update_whisper_file("stats.timers." + name + ".lower", whisper.Min, float64(td.min), t)
    update_whisper_file("stats.timers." + name + ".upper", whisper.Max, float64(td.max), t)
    update_whisper_file("stats.timers." + name + ".mean", whisper.Average, float64(td.mean), t)
    update_whisper_file("stats.timers." + name + ".median", whisper.Average, float64(td.q_50), t)
    update_whisper_file("stats.timers." + name + ".upper_75", whisper.Average, float64(td.q_75), t)
    update_whisper_file("stats.timers." + name + ".upper_90", whisper.Average, float64(td.q_90), t)
    update_whisper_file("stats.timers." + name + ".upper_95", whisper.Average, float64(td.q_95), t)
}

func ensure_rrd_dir_exists() {
    if _, err := os.Stat(*whisperPath); err == nil {
        return
    }
    os.Mkdir(*whisperPath, 0755)
}

func metric_to_file_path(metric string) string {
    path := *whisperPath + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
    return path
}


func ensure_whisper_file_exists(metric string, aggregation_method whisper.AggregationMethod) *whisper.Whisper {
    path := metric_to_file_path(metric)
    dir := filepath.Dir(path)
    err := os.MkdirAll(dir, 0755)
    if err != nil {
        log.Fatalf("Cannot create directory %s: %s\n", dir, err)
    }
    if file_exists(path) {
        w, err := whisper.Open(path)
        if err != nil {
            log.Fatalf("Cannot open Whisper file %s: %s\n", path, err)
        }
        return w
    }
    xFilesFactor := 0.5
    w, err := whisper.Create(path, parsedArchiveParams, aggregation_method, float32(xFilesFactor))
    if err != nil {
        log.Fatalf("Cannot create Whisper file %s: %s\n", path, err)
    }
    return w
}

func update_whisper_file(metric string, aggregation_method whisper.AggregationMethod, value float64, timestamp uint32) {
    wh := ensure_whisper_file_exists(metric, aggregation_method)
    err := wh.Update(value, int(timestamp))
    if err != nil {
        log.Fatalf("Cannot update Whisper file for metric %s: %s\n", metric, err)
    }
}
