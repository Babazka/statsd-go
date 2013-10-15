// +build rrd

package main

import (
	"os"
	"./rrd"
	"./embedded"
	"log"
	"io/ioutil"
	"fmt"
	"net/http"
	"strings"
	"strconv"
	"time"
	"flag"
)

const (
    RRD_STEP = 10
    RRD_DIR = "data"
)

var (
	webAddress       = flag.String("webface", ":5400", "HTTP web interface address")
)

type RrdBackend struct {
}

func NewRrdBackend() *RrdBackend {
    var b RrdBackend;
	go rrdHttpServer()
    log.Printf("Writing to RRD files at %s/\n", RRD_DIR)
    return &b;
}

func (b *RrdBackend) beginAggregation() {
    ensure_rrd_dir_exists()
}
func (b *RrdBackend) endAggregation() {
}
func (b *RrdBackend) handleCounter(name string, count int64, count_ps float64) {
    write_to_gauge_rrd(name, count)
}
func (b *RrdBackend) handleGauge(name string, count int64) {
    write_to_gauge_rrd(name, count)
}
func (b *RrdBackend) handleTiming(name string, td TimerDistribution) {
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

func get_metric_type(metric string) string {
    if file_exists(mk_metric_filename(metric + ".gauge")) {
        return "gauge"
    }
    if file_exists(mk_metric_filename(metric + ".timing")) {
        return "timing"
    }
    return ""
}

func http_metric_page(w http.ResponseWriter, r *http.Request, metric string, metric_type string, path_tail string, minutes int) {
    w.Header().Set("Content-type", "text/html")
    if metric_type == "timing" {
        fmt.Fprintf(w, "<img src=\"/%s/timing/%s/%d/\">", metric, path_tail, minutes)
    }
    if file_exists(mk_metric_filename(metric + ".bad.gauge")) {
        fmt.Fprintf(w, "<img src=\"/%s/gaugebad/%s/%d\">", metric, path_tail, minutes)
    } else {
        fmt.Fprintf(w, "<img src=\"/%s/gauge/%s/%d\">", metric, path_tail, minutes)
    }
    fmt.Fprintf(w, "<br/>")
}

func concat(old1, old2 []string) []string {
    newslice := make([]string, len(old1) + len(old2))
    copy(newslice, old1)
    copy(newslice[len(old1):], old2)
    return newslice
}

func get_int_param(r *http.Request, name string) (int, bool) {
	v_arr, ok := r.URL.Query()[name]
	if !ok {
		return 0, false
	}
	v_i, err := strconv.Atoi(v_arr[0])
	if err != nil {
		return 0, false
	}
	return v_i, true
}

func http_main(w http.ResponseWriter, r *http.Request) {
    path := strings.Split(r.URL.Path[1:], "/")

    if path[0] == "favicon.ico" {
        w.Header().Set("Content-type", "image/x-icon")
        w.Write(embedded.Favicon_ico())
        return
    }

	period_minutes, ok := get_int_param(r, "minutes")
	if !ok {
		period_minutes = 30
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
            if !strings.HasPrefix(file.Name(), search_prefix) {
                continue
            }
            if strings.HasSuffix(file.Name(), ".timing.rrd") {
                metrics = append(metrics, strings.Replace(file.Name(), ".timing.rrd", "", 1))
                continue
            }
            if strings.HasSuffix(file.Name(), ".gauge.rrd") {
                metrics = append(metrics, strings.Replace(file.Name(), ".gauge.rrd", "", 1))
                continue
            }
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
                http_metric_page(w, r, metric, metric_type, strings.Join(path[2:], "/"), period_minutes)
                continue
            }
            metric_type = path[1]
        }

        t := time.Now()
        minutes := period_minutes;

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
            g.SetVLabel("value")
            g.Def("g", filename, "num", "AVERAGE")
            g.VDef("v_max", "g,MAXIMUM")
            g.VDef("v_min", "g,MINIMUM")
            g.VDef("v_avg", "g,AVERAGE")
            /*g.VDef("v_q90", "g,90,PERCENTNAN")*/
            g.Area("g", "eeffee")
            g.Line(2, "g", "008800")
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
            g.SetVLabel("events / second")
            g.Def("g", filename, "num", "AVERAGE")
            g.Def("b", bad_filename, "num", "AVERAGE")
            g.CDef("gpm", fmt.Sprintf("g,%d,*", 1))
            g.CDef("bpm", fmt.Sprintf("b,%d,*", 1))
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

        g.SetSize(600, 130)

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

func rrdHttpServer() {
    log.Printf("Web interface available at %s", *webAddress)
    http.HandleFunc("/", http_main)
    http.ListenAndServe(*webAddress, nil)
}

