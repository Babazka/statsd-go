// +build !rrd

package main

//import (
//	"os"
//	"log"
//	"io/ioutil"
//	"fmt"
//	"net/http"
//	"strings"
//	"time"
//	"flag"
//)

const (
    RRD_STEP = 30
)

type RrdBackend struct {
}

func NewRrdBackend() *RrdBackend {
    var b RrdBackend;
    return &b;
}

func (b *RrdBackend) beginAggregation() {
}
func (b *RrdBackend) endAggregation() {
}
func (b *RrdBackend) handleCounter(name string, count int64, count_ps float64) {
}
func (b *RrdBackend) handleGauge(name string, count int64) {
}
func (b *RrdBackend) handleTiming(name string, td TimerDistribution) {
}
