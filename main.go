package main

import (
	"os"
	"bytes"
	"io/ioutil"
	"flag"
	"fmt"
	"./gmetric"
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

const (
    STEP = 30
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
	flushInterval    = flag.Int64("flush-interval", STEP, "Flush interval")
	percentThreshold = flag.Int("percent-threshold", 90, "Threshold percent")
	debug            = flag.Bool("debug", false, "Debug mode")
)

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

func ensure_rrd_dir_exists() {
    if _, err := os.Stat(RRD_DIR); err == nil {
        return
    }
    os.Mkdir(RRD_DIR, 0755)
}

func mk_common_rrd(filename string) *rrd.Creator {
    t := time.Unix(time.Now().Unix() - STEP, 0)
    c := rrd.NewCreator(filename, t, STEP)
    c.RRA("AVERAGE", 0.5, 1,         4 * 60 * 60 / STEP)
    c.RRA("AVERAGE", 0.5, 5,         3 * 24 * 60 * 60 / (5 * STEP))
    c.RRA("AVERAGE", 0.5, 30,       31 * 24 * 60 * 60 / (30 * STEP))
    c.RRA("AVERAGE", 0.5, 6 * 60,    3 * 31 * 24 * 60 * 60 / (6 * 60 * STEP))
    c.RRA("AVERAGE", 0.5, 24 * 60, 365 * 24 * 60 * 60 / (24 * 60 * STEP))
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
    c.DS("num", "GAUGE", 2 * STEP, 0, 2147483647)
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
    c.DS("min", "GAUGE", 2 * STEP, 0, 2147483647)
    c.DS("max", "GAUGE", 2 * STEP, 0, 2147483647)
    c.DS("avg", "GAUGE", 2 * STEP, 0, 2147483647)
    c.DS("med", "GAUGE", 2 * STEP, 0, 2147483647)
    c.DS("q90", "GAUGE", 2 * STEP, 0, 2147483647)
    c.DS("num", "GAUGE", 2 * STEP, 0, 2147483647)
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

func monitor() {
	var err error
	if err != nil {
		log.Println(err)
	}
	t := time.NewTicker(time.Duration(*flushInterval) * time.Second)
	for {
		select {
		case <-t.C:
			submit()
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

func submit() {
	var clientGraphite net.Conn

    ensure_rrd_dir_exists()

	if *graphiteAddress != "" {
		var err error
		clientGraphite, err = net.Dial(TCP, *graphiteAddress)
		if clientGraphite != nil {
			// Run this when we're all done, only if clientGraphite was opened.
			defer clientGraphite.Close()
		}
		if err != nil {
			log.Printf(err.Error())
		}
	}
	var useGanglia bool
	var gm gmetric.Gmetric
	gmSubmit := func(name string, value uint32) {
		if useGanglia {
			if *debug {
				log.Println("Ganglia send metric %s value %d\n", name, value)
			}
			m_value := fmt.Sprint(value)
			m_units := "count"
			m_type := uint32(gmetric.VALUE_UNSIGNED_INT)
			m_slope := uint32(gmetric.SLOPE_BOTH)
			m_grp := "statsd"
			m_ival := uint32(*flushInterval * int64(2))

			go gm.SendMetric(name, m_value, m_type, m_units, m_slope, m_ival, m_ival, m_grp)
		}
	}
	gmSubmitFloat := func(name string, value float64) {
		if useGanglia {
			if *debug {
				log.Println("Ganglia send float metric %s value %f\n", name, value)
			}
			m_value := fmt.Sprint(value)
			m_units := "count"
			m_type := uint32(gmetric.VALUE_DOUBLE)
			m_slope := uint32(gmetric.SLOPE_BOTH)
			m_grp := "statsd"
			m_ival := uint32(*flushInterval * int64(2))

			go gm.SendMetric(name, m_value, m_type, m_units, m_slope, m_ival, m_ival, m_grp)
		}
	}
	if *gangliaAddress != "" {
		gm = gmetric.Gmetric{
			Host:  *gangliaSpoofHost,
			Spoof: *gangliaSpoofHost,
		}
		gm.SetVerbose(false)

		if strings.Contains(*gangliaAddress, ",") {
			segs := strings.Split(*gangliaAddress, ",")
			for i := 0; i < len(segs); i++ {
				gIP, err := net.ResolveIPAddr("ip4", segs[i])
				if err != nil {
					panic(err.Error())
				}
				gm.AddServer(gmetric.GmetricServer{gIP.IP, *gangliaPort})
			}
		} else {
			gIP, err := net.ResolveIPAddr("ip4", *gangliaAddress)
			if err != nil {
				panic(err.Error())
			}
			gm.AddServer(gmetric.GmetricServer{gIP.IP, *gangliaPort})
		}
		useGanglia = true
	} else {
		useGanglia = false
	}
	numStats := 0
	now := time.Now()
	buffer := bytes.NewBufferString("")
	for s, c := range counters {
		value := float64(c) / float64((float64(*flushInterval)*float64(time.Second))/float64(1e3))
		fmt.Fprintf(buffer, "stats.%s %d %d\n", s, value, now)
		gmSubmitFloat(fmt.Sprintf("stats_%s", s), value)
		fmt.Fprintf(buffer, "stats_counts.%s %d %d\n", s, c, now)
		gmSubmit(fmt.Sprintf("stats_counts_%s", s), uint32(c))
        write_to_gauge_rrd(s, int64(c))
		counters[s] = 0
		numStats++
	}
	for i, g := range gauges {
		value := int64(g)
		fmt.Fprintf(buffer, "stats.%s %d %d\n", i, value, now)
		gmSubmit(fmt.Sprintf("stats_%s", i), uint32(value))
        write_to_gauge_rrd(i, value)
        gauges[i] = 0  // why wasn't it in the original?
		numStats++
	}
	for u, t := range timers {
		if len(t) > 0 {
			sort.Float64s(t)
			min := float64(t[0])
			max := float64(t[len(t)-1])
			med := float64(t[len(t)/2])
			mean := float64(min)
            q90 := float64(t[len(t)*9/10])
			maxAtThreshold := float64(max)
			count := len(t)
			count_ps := float64(count) / float64(*flushInterval)
			if len(t) > 1 {
				var thresholdIndex int
				thresholdIndex = ((100 - *percentThreshold) / 100) * count
				numInThreshold := count - thresholdIndex
				values := t[0:numInThreshold]

				sum := float64(0)
				for i := 0; i < numInThreshold; i++ {
					sum += values[i]
				}
				mean = float64(sum) / float64(numInThreshold)
			}
			var z []float64
			timers[u] = z

			fmt.Fprintf(buffer, "stats.timers.%s.mean %f %d\n", u, mean, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_mean", u), mean)
			fmt.Fprintf(buffer, "stats.timers.%s.upper %f %d\n", u, max, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_upper", u), max)
			fmt.Fprintf(buffer, "stats.timers.%s.upper_%d %f %d\n", u,
				*percentThreshold, maxAtThreshold, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_upper_%d", u, *percentThreshold), maxAtThreshold)
			fmt.Fprintf(buffer, "stats.timers.%s.lower %f %d\n", u, min, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_lower", u), min)
			fmt.Fprintf(buffer, "stats.timers.%s.count %d %d\n", u, count, now)
			fmt.Fprintf(buffer, "stats.timers.%s.count_ps %f %d\n", u, count_ps, now)
			gmSubmit(fmt.Sprintf("stats_timers_%s_count", u), uint32(count))

            write_to_timing_rrd(u, min, max, mean, med, q90, count);
		} else {
			// Need to still submit timers as zero
			fmt.Fprintf(buffer, "stats.timers.%s.mean %f %d\n", u, 0, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_mean", u), 0)
			fmt.Fprintf(buffer, "stats.timers.%s.upper %f %d\n", u, 0, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_upper", u), 0)
			fmt.Fprintf(buffer, "stats.timers.%s.upper_%d %f %d\n", u,
				*percentThreshold, 0, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_upper_%d", u, *percentThreshold), 0)
			fmt.Fprintf(buffer, "stats.timers.%s.lower %f %d\n", u, 0, now)
			gmSubmitFloat(fmt.Sprintf("stats_timers_%s_lower", u), 0)
			fmt.Fprintf(buffer, "stats.timers.%s.count %d %d\n", u, 0, now)
			fmt.Fprintf(buffer, "stats.timers.%s.count_ps %f %d\n", u, 0, now)
			gmSubmit(fmt.Sprintf("stats_timers_%s_count", u), uint32(0))
            write_to_timing_rrd(u, 0, 0, 0, 0, 0, 0);
		}
		numStats++
	}
	fmt.Fprintf(buffer, "statsd.numStats %d %d\n", numStats, now)
	gmSubmit("statsd_numStats", uint32(numStats))
	if clientGraphite != nil {
		if *debug {
			log.Println("Send to graphite: [[[%s]]]\n", string(buffer.Bytes()))
		}
		clientGraphite.Write(buffer.Bytes())
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
            g.CDef("gpm", fmt.Sprintf("g,%d,*", 60 / STEP))
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
            g.CDef("gpm", fmt.Sprintf("g,%d,*", 60 / STEP))
            g.CDef("bpm", fmt.Sprintf("b,%d,*", 60 / STEP))
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
