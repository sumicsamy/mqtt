package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

/************** Telemetry Schema **************/
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
	GeoJSON map[string]interface{} `json:"geojson"`
}

/************** Prometheus Metrics **************/

var (
	msgCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "kafka_telemetry_messages_total"},
		[]string{"region", "truck"},
	)

	msgRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_telemetry_messages_per_second"},
		[]string{"region", "truck"},
	)

	latencyHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "telemetry_delivery_latency_seconds",
			Buckets: prometheus.LinearBuckets(0.1, 0.25, 20),
		},
		[]string{"region", "truck"},
	)

	truckSpeed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "truck_speed_kph"},
		[]string{"region", "truck"},
	)

	truckFuel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "truck_fuel_percent"},
		[]string{"region", "truck"},
	)

	truckLoad = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "truck_payload_tonnes"},
		[]string{"region", "truck"},
	)

	truckLat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "truck_latitude"},
		[]string{"region", "truck"},
	)

	truckLon = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "truck_longitude"},
		[]string{"region", "truck"},
	)

	backpressureEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "kafka_backpressure_events_total"},
		[]string{"region", "truck"},
	)
)

/************** Init Prometheus **************/
func init() {
	prometheus.MustRegister(
		msgCount, msgRate, latencyHist,
		truckSpeed, truckFuel, truckLoad,
		truckLat, truckLon,
		backpressureEvents,
	)
}

/************** Loki Push **************/
func pushToLoki(entry map[string]interface{}) {
	lokiURL := os.Getenv("LOKI_PUSH_URL") // e.g. http://loki-gateway.openshift-logging.svc:3100/loki/api/v1/push
	if lokiURL == "" {
		return
	}

	line, _ := json.Marshal(entry)

	body := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{
					"app":    "fleet-telemetry-consumer",
					"region": entry["region"].(string),
					"truck":  entry["truck"].(string),
				},
				"values": [][]string{
					{fmt.Sprintf("%d", time.Now().UnixNano()), string(line)},
				},
			},
		},
	}

	b, _ := json.Marshal(body)
	http.Post(lokiURL, "application/json", bytes.NewBuffer(b))
}

/************** Kafka Consumer **************/
type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	last := time.Now()
	count := 0

	for msg := range claim.Messages() {
		var t Telemetry
		json.Unmarshal(msg.Value, &t)

		labels := []string{t.Region, t.Truck}

		// ---------------------- Prometheus Metrics ----------------------
		msgCount.WithLabelValues(labels...).Inc()

		// Speed, fuel, load, coords
		truckSpeed.WithLabelValues(labels...).Set(t.Spd)
		truckFuel.WithLabelValues(labels...).Set(t.Eng.Fuel)
		truckLoad.WithLabelValues(labels...).Set(t.Load.PayT)
		truckLat.WithLabelValues(labels...).Set(t.Loc.Lat)
		truckLon.WithLabelValues(labels...).Set(t.Loc.Lon)

		// End to end latency
		ts, _ := time.Parse(time.RFC3339, t.TS)
		latency := time.Since(ts).Seconds()
		latencyHist.WithLabelValues(labels...).Observe(latency)

		// Backpressure threshold
		if latency > 0.5 { // 500 ms
			backpressureEvents.WithLabelValues(labels...).Inc()
		}

		// Message rate
		count++
		if time.Since(last) > 1*time.Second {
			msgRate.WithLabelValues(labels...).Set(float64(count))
			count = 0
			last = time.Now()
		}

		// ---------------------- Loki Structured Log ----------------------
		geo := map[string]interface{}{
			"ts":      t.TS,
			"region":  t.Region,
			"truck":   t.Truck,
			"lat":     t.Loc.Lat,
			"lon":     t.Loc.Lon,
			"speed":   t.Spd,
			"payload": t.Load.PayT,
			"fuel":    t.Eng.Fuel,
			"state":   t.GeoJSON["properties"].(map[string]interface{})["state"],
			"feature": t.GeoJSON, // Full GeoJSON Feature
		}

		fmt.Println(toJson(geo))
		pushToLoki(geo)

		sess.MarkMessage(msg, "")
	}

	return nil
}

func toJson(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

/************** MAIN **************/
func main() {
	// Start Prometheus server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8081", nil)
	}()

	brokers := getenv("KAFKA_BROKERS", "fleet-kafka-kafka-bootstrap:9092")
	topic := getenv("KAFKA_TOPIC", "truck-telemetry")
	group := getenv("KAFKA_GROUP", "fleet-consumer")

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	cg, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, cfg)
	if err != nil {
		log.Fatal("consumer group:", err)
	}

	ctx := context.Background()
	for {
		if err := cg.Consume(ctx, []string{topic}, &ConsumerGroupHandler{}); err != nil {
			log.Println("consume error:", err)
		}
	}
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
