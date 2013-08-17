package main

import (
	"bytes"
	"flag"
	"log"
	"runtime/pprof"
	"os"
	"net"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	TCP = "tcp"
	UDP = "udp"
)


type Packet struct {
	Bucket   string
	Value    string
	Modifier string
	Sampling float32
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	fwdToAddress     = flag.String("fwd-to", "", "Forward UDP packets to this address")
	graphiteAddress  = flag.String("graphite", "", "Graphite service address (example: 'localhost:2003')")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval")
	debug            = flag.Bool("debug", false, "Debug mode")
	useRrdBackend    = flag.Bool("rrd", false, "Store data in RRDs or Whisper files")
    cpuprofile       = flag.String("cpuprofile", "", "Write cpu profile to this file")
)

type TimerDistribution struct {
    count int
    count_ps float64
    mean float64
    min float64
    q_50 float64
    q_75 float64
    q_90 float64
    q_95 float64
    max float64
}

type StatsdBackend interface {
    beginAggregation()
    handleCounter(name string, count int64, count_ps float64)
    handleGauge(name string, count int64)
    handleTiming(name string, params TimerDistribution)
    endAggregation()
}

var (
	In       = make(chan Packet, 10000)
	counters = make(map[string]int)
	timers   = make(map[string][]float64)
	gauges   = make(map[string]int)
)


func buildBackends() []StatsdBackend {
    var backends []StatsdBackend
    if *useRrdBackend {
        backends = append(backends, NewRrdBackend())
    }
    if *graphiteAddress != "" {
        backends = append(backends, NewGraphiteBackend(*graphiteAddress))
    }
    if len(backends) == 0 {
        log.Printf("WARNING: No backends specified. Data will be lost.\n")
    }
    return backends
}

func monitor() {
    backends := buildBackends()
	t := time.NewTicker(time.Duration(*flushInterval) * time.Second)
	for {
		select {
		case <-t.C:
			submit(backends)
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t []float64
					timers[s.Bucket] = t
				}
				floatValue, _ := strconv.ParseFloat(s.Value, 32)
				timers[s.Bucket] = append(timers[s.Bucket], floatValue)
			} else if s.Modifier == "g" {
                floatValue, _ := strconv.ParseFloat(s.Value, 32)
				intValue := int(floatValue)
				gauges[s.Bucket] = intValue
			} else {
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				floatValue, _ := strconv.ParseFloat(s.Value, 32)
				counters[s.Bucket] += int(float32(floatValue) * (1 / s.Sampling))
			}
		}
	}
}

func submit(backends []StatsdBackend) {
    for _, bk := range backends {
        bk.beginAggregation()
    }

	numStats := 0
	for s, c := range counters {
		value := float64(c) / float64(*flushInterval)
		counters[s] = 0
        for _, bk := range backends {
            bk.handleCounter(s, int64(c), value)
        }
		numStats++
	}
	for i, g := range gauges {
		value := int64(g)
        for _, bk := range backends {
            bk.handleGauge(i, value)
        }
		numStats++
	}
	for u, t := range timers {
        var td TimerDistribution;
		if len(t) > 0 {
            float_len := float64(len(t))
			sort.Float64s(t)
			td.min = float64(t[0])
			td.max = float64(t[len(t)-1])
			td.q_50 = float64(t[len(t)/2])
            td.q_75 = float64(t[int(float_len*0.75)])
            td.q_90 = float64(t[int(float_len*0.90)])
            td.q_95 = float64(t[int(float_len*0.95)])
			td.count = len(t)
			td.count_ps = float64(len(t)) / float64(*flushInterval)

            sum := float64(0)
            for i := 0; i < len(t); i++ {
                sum += t[i]
            }
            td.mean = float64(sum) / float64(td.count)

			var z []float64
			timers[u] = z
		} else {
			td.min = 0
			td.max = 0
			td.q_50 = 0
			td.mean = 0
            td.q_75 = 0
            td.q_90 = 0
            td.q_95 = 0
			td.count = 0
			td.count_ps = 0
		}
        for _, bk := range backends {
            bk.handleTiming(u, td)
        }
		numStats++
	}
    for _, bk := range backends {
        bk.endAggregation()
    }
}

var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_\\.\\-]+):(\\-?[0-9\\.]+)\\|(c|g|ms)(\\|@([0-9\\.]+))?")

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var value string
	/*s := sanitizeRegexp.ReplaceAllString(buf.String(), "")*/
    s := buf.String()
	for _, item := range packetRegexp.FindAllStringSubmatch(s, -1) {
		value = item[2]
		if item[3] == "ms" {
			_, err := strconv.ParseFloat(item[2], 32)
			if err != nil {
				value = "0"
			}
		}

		sampleRate, err := strconv.ParseFloat(item[5], 32)
		if err != nil {
			sampleRate = 1
		}

		packet.Bucket = item[1]
		packet.Value = value
		packet.Modifier = item[3]
		packet.Sampling = float32(sampleRate)

		if *debug {
			log.Printf("Packet: bucket = %s, value = %s, modifier = %s, sampling = %f\n", packet.Bucket, packet.Value, packet.Modifier, packet.Sampling)
		}

		In <- packet
	}

    //packet.Bucket = "statsd.packets_received"
    //packet.Value = "1"
    //packet.Modifier = "c"
    //packet.Sampling = 1
    //In <- packet

    packet.Bucket = "statsd-monitor.packets_received"
    packet.Value = "1"
    packet.Modifier = "c"
    packet.Sampling = 1
    In <- packet
}

func udpListener() {
    var fwdConn *net.UDPConn
    var fwdToAddr *net.UDPAddr
    if *fwdToAddress != "" {
        var e error
        fwdToAddr, e = net.ResolveUDPAddr(UDP, *fwdToAddress)
        if e != nil {
            log.Fatalf("Cannot resolve forwarding address '%s'", *fwdToAddress)
        }
        fwdConn, e = net.DialUDP(UDP, nil, fwdToAddr)
        if e != nil {
            log.Fatalf("Cannot connect to '%s' via UDP (whatever what means in Go)", *fwdToAddress)
        }
    }
	address, _ := net.ResolveUDPAddr(UDP, *serviceAddress)
	listener, err := net.ListenUDP(UDP, address)
	defer listener.Close()
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}

    log.Printf("Listening to UDP at %s", *serviceAddress)
    if fwdConn != nil {
        log.Printf("Forwarding UDP traffic to %s", *fwdToAddress)
    }

	for {
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
        if fwdConn != nil {
            fwdConn.Write(message[0:n])
        }
		buf := bytes.NewBuffer(message[0:n])
		if *debug {
			log.Println("Packet received: " + string(message[0:n]))
		}
		go handleMessage(listener, remaddr, buf)
	}
}

func main() {
	flag.Parse()

    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

	go udpListener()
	monitor()
}
