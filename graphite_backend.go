package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
)

type GraphiteBackend struct {
    now int64
    buffer *bytes.Buffer
    clientGraphite net.Conn
    graphiteAddress string
}

func NewGraphiteBackend(graphiteAddress string) *GraphiteBackend {
    var b GraphiteBackend
    b.graphiteAddress = graphiteAddress
    return &b
}

func (b *GraphiteBackend) beginAggregation() {
    var err error
	b.now = time.Now().Unix()
    b.buffer = bytes.NewBufferString("")
    b.clientGraphite, err = net.Dial(TCP, b.graphiteAddress)
    if err != nil {
        log.Printf(err.Error())
    }
}
func (b *GraphiteBackend) endAggregation() {
    if b.clientGraphite != nil {
        b.clientGraphite.Write(b.buffer.Bytes())
        b.clientGraphite.Close()
    }
}

func (b *GraphiteBackend) handleCounter(name string, count int64, count_ps float64) {
    fmt.Fprintf(b.buffer, "stats.%s %d %d\n", name, count_ps, b.now)
    fmt.Fprintf(b.buffer, "stats_counts.%s %d %d\n", name, count, b.now)
}
func (b *GraphiteBackend) handleGauge(name string, v float64) {
    fmt.Fprintf(b.buffer, "stats.%s %f %d\n", name, v, b.now)
}
func (b *GraphiteBackend) handleTiming(name string, td TimerDistribution) {
    fmt.Fprintf(b.buffer, "stats.timers.%s.mean %f %d\n",     name, td.mean, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.upper %f %d\n",    name, td.max, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.upper_%d %f %d\n", name, 75, td.q_75, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.upper_%d %f %d\n", name, 90, td.q_90, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.upper_%d %f %d\n", name, 95, td.q_95, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.lower %f %d\n",    name, td.min, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.count %d %d\n",    name, td.count, b.now)
    fmt.Fprintf(b.buffer, "stats.timers.%s.count_ps %f %d\n", name, td.count_ps, b.now)
}
