package main

import (
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

// TelemetryFragment handles the fragmented JSON payloads from the bridge.
// We use pointers (*Type) to detect which fields are present in the message.
type TelemetryFragment struct {
	TS string `json:"ts"`

	// --- Location Payload ---
	// "lat" is shared with System (latency), so we inspect context
	// "lon", "alt" are unique to Location
	Lat *float64 `json:"lat"`
	Lon *float64 `json:"lon"`
	Alt *float64 `json:"alt"`

	// --- Engine Payload ---
	Rpm *int `json:"rpm"`
	// "tmp" is shared with System, so we inspect context
	Tmp  *float64 `json:"tmp"`
	Oil  *float64 `json:"oil"`
	Fuel *float64 `json:"fuel"`

	// --- Load Payload ---
	GrossT *float64 `json:"gross_t"`
	PayT   *float64 `json:"pay_t"`
	Tray   *float64 `json:"tray"`

	// --- System Payload ---
	Cpu *int `json:"cpu"`
	Mem *int `json:"mem"`
	// Tmp (System Temp) - shared key
	// Lat (Network Latency) - shared key
	Sig *int `json:"sig"`
	// New simulator uses specific key for network latency
	NetworkLatency *int `json:"network_latency"`

	// --- Status Payload ---
	State      *string  `json:"state"`
	Spd        *float64 `json:"spd"`
	RateHz     *float64 `json:"rate_hz"`
	TargetID   *int     `json:"target_id"`
	TargetType *string  `json:"target_type"`
}

/************** Prometheus Metrics **************/

var (
	msgCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_telemetry_messages_total",
			Help: "Total number of telemetry messages consumed from Kafka",
		},
		[]string{"region", "truck"},
	)

	msgRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_telemetry_messages_per_second",
			Help: "Approx message rate per truck (msgs/sec) over a 1s window",
		},
		[]string{"region", "truck"},
	)

	latencyHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "telemetry_delivery_latency_seconds",
			Help:    "End-to-end delivery latency from telemetry TS to consumer time",
			Buckets: prometheus.LinearBuckets(0.1, 0.25, 20),
		},
		[]string{"region", "truck"},
	)

	truckSpeed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_speed_kph",
			Help: "Truck speed in km/h",
		},
		[]string{"region", "truck"},
	)

	truckFuel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_fuel_percent",
			Help: "Truck fuel level percentage",
		},
		[]string{"region", "truck"},
	)

	truckLoad = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_payload_tonnes",
			Help: "Truck payload in tonnes",
		},
		[]string{"region", "truck"},
	)

	truckLat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_latitude",
			Help: "Truck latitude in decimal degrees",
		},
		[]string{"region", "truck"},
	)

	truckLon = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_longitude",
			Help: "Truck longitude in decimal degrees",
		},
		[]string{"region", "truck"},
	)

	backpressureEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_backpressure_events_total",
			Help: "Count of messages whose delivery latency exceeded the backpressure threshold",
		},
		[]string{"region", "truck"},
	)

	// Engine metrics
	engineRPM = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_engine_rpm",
			Help: "Truck engine revolutions per minute",
		},
		[]string{"region", "truck"},
	)

	engineTemp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_engine_temp_celsius",
			Help: "Truck engine temperature in Celsius",
		},
		[]string{"region", "truck"},
	)

	engineOil = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_engine_oil_bar",
			Help: "Truck engine oil pressure (simulated in bar)",
		},
		[]string{"region", "truck"},
	)

	// System metrics
	sysCPU = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_sys_cpu_percent",
			Help: "Onboard system CPU utilisation percent",
		},
		[]string{"region", "truck"},
	)

	sysMem = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_sys_mem_percent",
			Help: "Onboard system memory utilisation percent",
		},
		[]string{"region", "truck"},
	)

	sysTemp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_sys_temp_celsius",
			Help: "Onboard system temperature in Celsius",
		},
		[]string{"region", "truck"},
	)

	sysNetLatency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_sys_network_latency_ms",
			Help: "Onboard system network latency in milliseconds",
		},
		[]string{"region", "truck"},
	)

	sysSignal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_sys_signal_dbm",
			Help: "Onboard radio signal strength in dBm",
		},
		[]string{"region", "truck"},
	)

	truckState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_state",
			Help: "Truck state gauge (queueing_loader, loading, driving_to_crusher, etc.)",
		},
		[]string{"region", "truck", "state"},
	)

	// New Metric for Target Tracking
	truckTarget = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "truck_target_id",
			Help: "ID of the crusher or loader the truck is targeting",
		},
		[]string{"region", "truck", "target_type"},
	)
)

var (
	loaderLat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loader_latitude"},
		[]string{"region", "loader_id"},
	)
	loaderLon = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "loader_longitude"},
		[]string{"region", "loader_id"},
	)

	crusherLat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "crusher_latitude"},
		[]string{"region", "crusher_id"},
	)
	crusherLon = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "crusher_longitude"},
		[]string{"region", "crusher_id"},
	)
)

func pushStaticSiteLocations() {
	sites := map[string]struct {
		loaders  []struct{ Lat, Lon float64 }
		crushers []struct{ Lat, Lon float64 }
	}{
		"pilbara": {
			loaders: []struct{ Lat, Lon float64 }{
				{-22.29500, 117.76500},
				{-22.29620, 117.77050},
				{-22.29350, 117.76800},
			},
			crushers: []struct{ Lat, Lon float64 }{
				{-22.30000, 117.78000},
				{-22.29200, 117.79000},
			},
		},
		"tomprice": {
			loaders: []struct{ Lat, Lon float64 }{
				{-22.69150, 117.80000},
				{-22.68880, 117.80850},
				{-22.68620, 117.79780},
			},
			crushers: []struct{ Lat, Lon float64 }{
				{-22.69500, 117.80500},
				{-22.68500, 117.78500},
			},
		},
		"paraburdoo": {
			loaders: []struct{ Lat, Lon float64 }{
				{-23.20520, 117.66500},
				{-23.20780, 117.67000},
				{-23.20200, 117.66250},
			},
			crushers: []struct{ Lat, Lon float64 }{
				{-23.21000, 117.67500},
				{-23.20000, 117.68000},
			},
		},
	}

	for region, v := range sites {
		for i, l := range v.loaders {
			id := fmt.Sprintf("%d", i)
			loaderLat.WithLabelValues(region, id).Set(l.Lat)
			loaderLon.WithLabelValues(region, id).Set(l.Lon)
		}
		for i, c := range v.crushers {
			id := fmt.Sprintf("%d", i)
			crusherLat.WithLabelValues(region, id).Set(c.Lat)
			crusherLon.WithLabelValues(region, id).Set(c.Lon)
		}
	}
}

// all possible states from simulator
var allStates = []string{
	"driving_to_crusher",
	"unloading",
	"driving_to_loader",
	"queueing_loader",
	"loading",
}

/************** Init Prometheus **************/
func init() {
	prometheus.MustRegister(
		msgCount, msgRate, latencyHist,
		truckSpeed, truckFuel, truckLoad,
		truckLat, truckLon,
		backpressureEvents,
		engineRPM, engineTemp, engineOil,
		sysCPU, sysMem, sysTemp, sysNetLatency, sysSignal,
		truckState, truckTarget, loaderLat, loaderLon, crusherLat, crusherLon,
	)
}

/************** Kafka Consumer **************/
type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	last := time.Now()
	count := 0

	for msg := range claim.Messages() {
		// 1. Identify Identity from Kafka Key (Region:Truck)
		key := string(msg.Key)
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			sess.MarkMessage(msg, "")
			continue
		}
		region := parts[0]
		truck := parts[1]
		labels := []string{region, truck}

		// 2. Unmarshal into generic fragment
		var t TelemetryFragment
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("json decode failed: %v", err)
			continue
		}

		// ---------------------- Prometheus Metrics ----------------------
		msgCount.WithLabelValues(labels...).Inc()

		// --- LOCATION Message ---
		// We use presence of "Lon" to confirm this is a Location message
		if t.Lon != nil && t.Lat != nil {
			truckLat.WithLabelValues(labels...).Set(*t.Lat)
			truckLon.WithLabelValues(labels...).Set(*t.Lon)
		}

		// --- ENGINE Message ---
		// "Rpm" is unique to engine
		if t.Rpm != nil {
			engineRPM.WithLabelValues(labels...).Set(float64(*t.Rpm))
			if t.Tmp != nil {
				engineTemp.WithLabelValues(labels...).Set(*t.Tmp)
			}
			if t.Oil != nil {
				engineOil.WithLabelValues(labels...).Set(*t.Oil)
			}
			if t.Fuel != nil {
				truckFuel.WithLabelValues(labels...).Set(*t.Fuel)
			}
		}

		// --- LOAD Message ---
		if t.GrossT != nil {
			truckLoad.WithLabelValues(labels...).Set(*t.PayT)
		}

		// --- SYSTEM Message ---
		// "Cpu" is unique to system
		if t.Cpu != nil {
			sysCPU.WithLabelValues(labels...).Set(float64(*t.Cpu))
			if t.Mem != nil {
				sysMem.WithLabelValues(labels...).Set(float64(*t.Mem))
			}

			// Check specifically for "network_latency" field first
			if t.NetworkLatency != nil {
				sysNetLatency.WithLabelValues(labels...).Set(float64(*t.NetworkLatency))
			} else if t.Lat != nil {
				// Backward compatibility if simulator sends 'lat' in system context
				sysNetLatency.WithLabelValues(labels...).Set(float64(*t.Lat))
			}

			if t.Sig != nil {
				sysSignal.WithLabelValues(labels...).Set(float64(*t.Sig))
			}
			// System uses 'tmp' for Board Temp
			if t.Tmp != nil {
				sysTemp.WithLabelValues(labels...).Set(*t.Tmp)
			}
		}

		// --- STATUS Message ---
		if t.State != nil {
			if t.Spd != nil {
				truckSpeed.WithLabelValues(labels...).Set(*t.Spd)
			}

			// Truck state: reset all, then set current to 1
			for _, s := range allStates {
				truckState.WithLabelValues(region, truck, s).Set(0)
			}
			truckState.WithLabelValues(region, truck, *t.State).Set(1)

			// Target Tracking
			if t.TargetID != nil && t.TargetType != nil {
				truckTarget.WithLabelValues(region, truck, *t.TargetType).Set(float64(*t.TargetID))
			}
		}

		// --- Common Metadata ---
		// End to end latency
		if t.TS != "" {
			ts, err := time.Parse(time.RFC3339, t.TS)
			if err == nil {
				latency := time.Since(ts).Seconds()
				if latency < 0 {
					latency = 0
				}
				latencyHist.WithLabelValues(labels...).Observe(latency)

				// Backpressure threshold
				if latency > 0.5 { // 500 ms
					backpressureEvents.WithLabelValues(labels...).Inc()
				}
			}
		}

		// Message rate approximation per truck
		count++
		if time.Since(last) > 1*time.Second {
			msgRate.WithLabelValues(labels...).Set(float64(count))
			count = 0
			last = time.Now()
		}

		sess.MarkMessage(msg, "")
	}

	return nil
}

/************** MAIN **************/
func main() {
	// Start Prometheus server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Serving metrics on :8081/metrics")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	pushStaticSiteLocations()

	// FIX: Use KAFKA_BOOTSTRAP_SERVERS matching the deployment env var
	brokers := getenv("KAFKA_BOOTSTRAP_SERVERS", "")
	if brokers == "" {
		// Fallback for local dev or if env var is actually KAFKA_BROKERS
		brokers = getenv("KAFKA_BROKERS", "fleet-kafka-kafka-bootstrap:9092")
	}

	topic := getenv("KAFKA_TOPIC", "truck-telemetry")
	group := getenv("KAFKA_GROUP", "fleet-consumer")

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	cg, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, cfg)
	if err != nil {
		log.Fatal("consumer group init error:", err)
	}
	defer cg.Close()

	ctx := context.Background()
	handler := &ConsumerGroupHandler{}

	log.Printf("Kafka consumer starting. Brokers=%s, Topic=%s, Group=%s", brokers, topic, group)

	for {
		if err := cg.Consume(ctx, []string{topic}, handler); err != nil {
			log.Println("consume error:", err)
			time.Sleep(2 * time.Second)
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
