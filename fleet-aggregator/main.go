package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Telemetry struct {
	TS     string `json:"ts"`
	Region string `json:"region"`
	Truck  string `json:"truck"`
	Loc    struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
		Alt float64 `json:"alt"`
	} `json:"loc"`
	Spd float64 `json:"spd"`
	Eng struct {
		Rpm  int     `json:"rpm"`
		Tmp  float64 `json:"tmp"`
		Oil  float64 `json:"oil"`
		Fuel float64 `json:"fuel"`
	} `json:"eng"`
	Load struct {
		GrossT float64 `json:"gross_t"`
		PayT   float64 `json:"pay_t"`
		Tray   float64 `json:"tray"`
	} `json:"load"`
	Sys struct {
		Cpu int     `json:"cpu"`
		Mem int     `json:"mem"`
		Tmp float64 `json:"tmp"`
		Lat int     `json:"lat"`
		Sig int     `json:"sig"`
	} `json:"sys"`
}

/************** Prometheus Metrics **************/

var (
	// Messages per second
	mps = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fleet_mps",
			Help: "Messages per second per truck",
		},
		[]string{"region", "truck"},
	)

	// Truck telemetry
	speed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_speed", Help: "Latest truck speed"},
		[]string{"region", "truck"},
	)

	rpmGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_rpm", Help: "Latest engine RPM"},
		[]string{"region", "truck"},
	)

	fuelGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_fuel_pct", Help: "Fuel percentage"},
		[]string{"region", "truck"},
	)

	payT = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_load_pay_t", Help: "Payload tonnes"},
		[]string{"region", "truck"},
	)

	// Location metrics for Grafana Geomap
	locLat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_loc_lat", Help: "Latitude"},
		[]string{"region", "truck"},
	)

	locLon = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_loc_lon", Help: "Longitude"},
		[]string{"region", "truck"},
	)

	locAlt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fleet_loc_alt", Help: "Altitude"},
		[]string{"region", "truck"},
	)
)

var (
	lock     sync.Mutex
	counters = map[string]int{}
)

func init() {
	prometheus.MustRegister(
		mps, speed, rpmGauge, fuelGauge, payT,
		locLat, locLon, locAlt,
	)
}

func main() {

	brokers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	topic := os.Getenv("KAFKA_TOPIC")
	port := os.Getenv("PROM_PORT")
	if port == "" {
		port = "8080"
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), "fleet-aggregator", cfg)
	if err != nil {
		log.Fatalf("Kafka consumer: %v", err)
	}

	go func() {
		for {
			group.Consume(nil, []string{topic}, handler{})
		}
	}()

	// MPS updater
	go func() {
		for {
			time.Sleep(time.Second)
			lock.Lock()
			for key, count := range counters {
				parts := strings.Split(key, ":")
				region, truck := parts[0], parts[1]
				mps.WithLabelValues(region, truck).Set(float64(count))
			}
			counters = map[string]int{}
			lock.Unlock()
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Serving metrics on :%s/metrics", port)
	http.ListenAndServe(":"+port, nil)
}

type handler struct{}

func (handler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (handler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {

		var t Telemetry
		if err := json.Unmarshal(msg.Value, &t); err == nil {

			key := t.Region + ":" + t.Truck

			lock.Lock()
			counters[key]++
			lock.Unlock()

			// Update values
			speed.WithLabelValues(t.Region, t.Truck).Set(t.Spd)
			rpmGauge.WithLabelValues(t.Region, t.Truck).Set(float64(t.Eng.Rpm))
			fuelGauge.WithLabelValues(t.Region, t.Truck).Set(t.Eng.Fuel)
			payT.WithLabelValues(t.Region, t.Truck).Set(t.Load.PayT)

			// Location for Geomap
			locLat.WithLabelValues(t.Region, t.Truck).Set(t.Loc.Lat)
			locLon.WithLabelValues(t.Region, t.Truck).Set(t.Loc.Lon)
			locAlt.WithLabelValues(t.Region, t.Truck).Set(t.Loc.Alt)
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
