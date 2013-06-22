package main

import (
	"os"
	"bytes"
	"io/ioutil"
	"flag"
	"fmt"
	"./rrd"
	"./embedded"
	"log"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	TCP = "tcp"
	UDP = "udp"
)

const (
    RRD_DIR = "data"
)

type Packet struct {
	Bucket   string
	Value    string
	Modifier string
	Sampling float32
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	webAddress       = flag.String("webface", ":5400", "HTTP web interface address")
	graphiteAddress  = flag.String("graphite", "", "Graphite service address (example: 'localhost:2003')")
	gangliaAddress   = flag.String("ganglia", "", "Ganglia gmond servers, comma separated")
	gangliaPort      = flag.Int("ganglia-port", 8649, "Ganglia gmond service port")
	gangliaSpoofHost = flag.String("ganglia-spoof-host", "", "Ganglia gmond spoof host string")
	flushInterval    = flag.Int64("flush-interval", 30, "Flush interval")
	percentThreshold = flag.Int("percent-threshold", 90, "Threshold percent")
	debug            = flag.Bool("debug", false, "Debug mode")
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

func file_exists(filename string) bool {
    if _, err := os.Stat(filename); err == nil {
        return true
    }
    return false
}

func buildBackends() []StatsdBackend {
    var backends []StatsdBackend
    backends = append(backends, NewRrdBackend())
    /* TODO use cmdline params */
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
				_, ok := gauges[s.Bucket]
				if !ok {
					gauges[s.Bucket] = 0
				}
				intValue, _ := strconv.Atoi(s.Value)
				gauges[s.Bucket] += intValue
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
        gauges[i] = 0  // why wasn't it in the original?
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
}

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var value string
	var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
	var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_\\.\\-]+):(\\-?[0-9\\.]+)\\|(c|g|ms)(\\|@([0-9\\.]+))?")
	s := sanitizeRegexp.ReplaceAllString(buf.String(), "")
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
}

func udpListener() {
	address, _ := net.ResolveUDPAddr(UDP, *serviceAddress)
	listener, err := net.ListenUDP(UDP, address)
	defer listener.Close()
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}
    log.Printf("Listening to UDP at %s", *serviceAddress)
	for {
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		if *debug {
			log.Println("Packet received: " + string(message[0:n]))
		}
		go handleMessage(listener, remaddr, buf)
	}
}

func get_metric_type(metric string) string {
    if file_exists(mk_metric_filename(metric + ".gauge")) {
        return "gauge"
    }
    if file_exists(mk_metric_filename(metric + ".timing")) {
        return "timing"
    }
    return ""
}

func http_metric_page(w http.ResponseWriter, r *http.Request, metric string, metric_type string, path_tail string) {
    w.Header().Set("Content-type", "text/html")
    if metric_type == "timing" {
        fmt.Fprintf(w, "<img src=\"/%s/timing/%s\">", metric, path_tail)
    }
    if file_exists(mk_metric_filename(metric + ".bad.gauge")) {
        fmt.Fprintf(w, "<img src=\"/%s/gaugebad/%s\">", metric, path_tail)
    } else {
        fmt.Fprintf(w, "<img src=\"/%s/gauge/%s\">", metric, path_tail)
    }
    fmt.Fprintf(w, "<br/>")
}

func concat(old1, old2 []string) []string {
    newslice := make([]string, len(old1) + len(old2))
    copy(newslice, old1)
    copy(newslice[len(old1):], old2)
    return newslice
}

func http_main(w http.ResponseWriter, r *http.Request) {
    path := strings.Split(r.URL.Path[1:], "/")

    if path[0] == "favicon.ico" {
        w.Header().Set("Content-type", "image/x-icon")
        w.Write(embedded.Favicon_ico())
        return
    }

    metrics := strings.Split(path[0], ";")

    if path[0] == "index" {
        search_prefix := ""

        if len(path) > 1 && len(path[1]) != 0  {
            search_prefix = path[1]
            path = concat([]string{"index", "html"}, path[2:])
        } else {
            path = concat([]string{"index", "html"}, path[1:])
        }

        files, err := ioutil.ReadDir(RRD_DIR)
        if err != nil {
            fmt.Fprintf(w, "Error while enumerating metrics: %s", err.Error())
            return
        }
        metrics = make([]string, 0)
        for _, file := range files {
            if !strings.HasSuffix(file.Name(), ".timing.rrd") {
                continue
            }
            if !strings.HasPrefix(file.Name(), search_prefix) {
                continue
            }
            metrics = append(metrics, strings.Replace(file.Name(), ".timing.rrd", "", 1))
        }
    }

    for _, metric := range metrics {
        metric_type := get_metric_type(metric)

        if metric_type == "" {
            fmt.Fprintf(w, "no such metric: %s", metric)
            return
        }

        filename := mk_metric_filename(metric + "." + metric_type)
        if len(path) > 1 {
            if path[1] == "html" {
                http_metric_page(w, r, metric, metric_type, strings.Join(path[2:], "/"))
                continue
            }
            metric_type = path[1]
        }

        t := time.Now()
        minutes := 30;

        if len(path) > 2 && len(path[2]) > 0 {
            m, err := strconv.Atoi(path[2])
            if err != nil {
                fmt.Fprintf(w, "third argument must be a number of minutes")
                return
            }
            minutes = m
        }

        g := rrd.NewGrapher()
        if metric_type == "gauge" {
            g.SetTitle(metric)
            g.SetLowerLimit(0)
            g.SetVLabel("events / minute")
            g.Def("g", filename, "num", "AVERAGE")
            g.CDef("gpm", fmt.Sprintf("g,%d,*", 60 / (*flushInterval)))
            g.VDef("v_max", "gpm,MAXIMUM")
            g.VDef("v_min", "gpm,MINIMUM")
            g.VDef("v_avg", "gpm,AVERAGE")
            /*g.VDef("v_q90", "gpm,90,PERCENTNAN")*/
            g.Area("gpm", "eeffee")
            g.Line(2, "gpm", "008800")
            g.GPrint("v_min", "min = %.0lf")
            g.GPrint("v_max", "max = %.0lf")
            g.GPrint("v_avg", "avg = %.0lf")
            /*g.GPrint("v_q90", "q90 = %.0lf")*/
        } else if metric_type == "gaugebad" {
            bad_filename := mk_metric_filename(metric + ".bad.gauge")
            if !file_exists(bad_filename) {
                fmt.Fprintf(w, "no such metric: %s.bad", metric)
                return
            }
            g.SetTitle(metric)
            g.SetLowerLimit(0)
            g.SetVLabel("events / minute")
            g.Def("g", filename, "num", "AVERAGE")
            g.Def("b", bad_filename, "num", "AVERAGE")
            g.CDef("gpm", fmt.Sprintf("g,%d,*", 60 / (*flushInterval)))
            g.CDef("bpm", fmt.Sprintf("b,%d,*", 60 / (*flushInterval)))
            g.CDef("ratio", "bpm,gpm,/,100,*")
            g.VDef("v_max", "gpm,MAXIMUM")
            g.VDef("v_min", "gpm,MINIMUM")
            g.VDef("v_avg", "gpm,AVERAGE")
            g.VDef("v_rat", "ratio,AVERAGE")
            /*g.VDef("v_q90", "gpm,90,PERCENTNAN")*/
            g.Area("gpm", "eeffee")
            g.Area("bpm", "ffeeee")
            g.Line(2, "gpm", "008800")
            g.Line(2, "bpm", "880000")
            g.GPrint("v_min", "min = %.0lf")
            g.GPrint("v_max", "max = %.0lf")
            g.GPrint("v_avg", "avg = %.0lf")
            /*g.GPrint("v_q90", "q90 = %.0lf")*/
            g.GPrint("v_rat", "bad = %.0lf%%")
        } else if metric_type == "timing" {
            g.SetTitle(metric)
            g.SetLowerLimit(0)
            g.SetVLabel("ms")
            g.SetUnitsExponent(0)
            g.Def("tmin", filename, "min", "AVERAGE")
            g.Def("tmax", filename, "max", "AVERAGE")
            g.Def("q50", filename, "med", "AVERAGE")
            g.Def("q90", filename, "q90", "AVERAGE")
            g.Def("tavg", filename, "avg", "AVERAGE")
            g.VDef("v_max", "tmax,MAXIMUM")
            g.VDef("v_min", "tmin,MINIMUM")
            g.VDef("v_avg", "tavg,AVERAGE")
            g.VDef("v_q90", "q90,AVERAGE")
            g.Area("q90", "eeeeff")
            g.Line(2, "tavg", "000088")
            g.Line(1, "q90", "000088")
            g.Line(1, "tmin", "bbbb88")
            /*g.Line(1, "tmax", "bbbb88")*/
            g.GPrint("v_min", "min = %.0lf")
            g.GPrint("v_max", "max = %.0lf")
            g.GPrint("v_avg", "avg = %.0lf")
            g.GPrint("v_q90", "q90 = %.0lf")
        }

        _, buf, err := g.Graph(t.Add(-time.Duration(60*minutes)*time.Second), t)
        if err != nil {
            fmt.Fprintf(w, "graph error: %s", err.Error())
            return
        }
        w.Header().Set("Content-type", "image/png")
        w.Write(buf)
        return
    }
}

func httpServer() {
    http.HandleFunc("/", http_main)
    http.ListenAndServe(*webAddress, nil)
}

func main() {
	flag.Parse()
	go udpListener()
	go httpServer()
	monitor()
}
