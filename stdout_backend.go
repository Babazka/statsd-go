package main

import (
	"fmt"
	"log"
	"strings"
)

type StdoutBackend struct {
	prefix string
}

func NewStdoutBackend(pattern string) *StdoutBackend {
    var b StdoutBackend;
	b.prefix = pattern
    log.Printf("Writing metrics starting with %s to stdout\n", pattern)
    return &b;
}

func (b *StdoutBackend) beginAggregation() {
}
func (b *StdoutBackend) endAggregation() {
}
func (b *StdoutBackend) handleCounter(name string, count int64, count_ps float64) {
	if strings.HasPrefix(name, b.prefix) {
		fmt.Printf("%s %d\n", name, count)
	}
}
func (b *StdoutBackend) handleGauge(name string, v float64) {
	if strings.HasPrefix(name, b.prefix) {
		fmt.Printf("%s %f\n", name, v)
	}
}
func (b *StdoutBackend) handleTiming(name string, td TimerDistribution) {
    //write_to_timing_rrd(name, td.min, td.max, td.mean, td.q_50, td.q_90, td.count);
}
