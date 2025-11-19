package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

/************** Message Schema **************/
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

/********************************************/

type Config struct {
	MqttBrokers     []string
	MqttTopicFilter string
	MqttQos         byte
	MqttClientID    string
	MqttUsername    string
	MqttPassword    string

	KafkaBrokers  []string
	KafkaTopic    string
	KafkaClientID string

	WorkerCount   int
	MessageBuffer int
	PromPort      string
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustInt(key, def string) int {
	v := getenv(key, def)
	i, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid int %s=%s", key, v)
	}
	return i
}

func loadConfig() *Config {
	cfg := &Config{
		MqttBrokers:     strings.Split(getenv("MQTT_BROKERS", "ssl://mqtt:8883"), ","),
		MqttTopicFilter: getenv("MQTT_TOPIC", "mine/fleet/+/truck/+/telemetry"),
		MqttClientID:    getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge"),
		MqttUsername:    getenv("MQTT_USERNAME", ""),
		MqttPassword:    getenv("MQTT_PASSWORD", ""),

		KafkaBrokers:  strings.Split(getenv("KAFKA_BROKERS", "fleet-kafka-kafka-bootstrap:9092"), ","),
		KafkaTopic:    getenv("KAFKA_TOPIC", "truck-telemetry"),
		KafkaClientID: getenv("KAFKA_CLIENT_ID", "mqtt-kafka-bridge"),

		WorkerCount:   mustInt("WORKER_COUNT", "8"),
		MessageBuffer: mustInt("MESSAGE_BUFFER", "100000"),
		PromPort:      getenv("PROM_PORT", "8080"),
	}

	if getenv("MQTT_QOS", "0") == "1" {
		cfg.MqttQos = 1
	} else {
		cfg.MqttQos = 0
	}

	return cfg
}

/************** Prometheus Metrics **************/
var (
	mqttIn = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_messages_in_total",
		Help: "MQTT messages received",
	})

	kafkaOut = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_out_total",
		Help: "Messages forwarded to Kafka",
	})

	kafkaErr = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_error_total",
		Help: "Kafka produce errors",
	})

	bufGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_buffer_current",
		Help: "Number of messages pending in buffer",
	})
)

func init() {
	prometheus.MustRegister(mqttIn, kafkaOut, kafkaErr, bufGauge)
}

/**************************************************/

type BridgeMessage struct {
	Key     string
	Payload []byte
}

func deriveKey(jsonPayload []byte) string {
	var t Telemetry
	if err := json.Unmarshal(jsonPayload, &t); err != nil {
		return "unknown:unknown"
	}
	return t.Region + ":" + t.Truck
}

func main() {
	cfg := loadConfig()

	msgCh := make(chan *BridgeMessage, cfg.MessageBuffer)

	// Kafka producer
	producer := newKafkaProducer(cfg)
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			kafkaErr.Inc()
			log.Printf("Kafka error: %v", err)
		}
	}()

	// Worker pool
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, producer, cfg.KafkaTopic, msgCh)
	}

	// MQTT client
	mqttClient := newMqttClient(cfg, msgCh)
	mqttClient.Connect()

	mqttClient.Subscribe(cfg.MqttTopicFilter, cfg.MqttQos, nil)
	log.Printf("Subscribed to MQTT pattern: %s", cfg.MqttTopicFilter)

	// Prom metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":"+cfg.PromPort, nil)
	}()

	// Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("Shutting down...")
	cancel()
	mqttClient.Disconnect(200)
	close(msgCh)
	wg.Wait()
}

func worker(ctx context.Context, wg *sync.WaitGroup, producer sarama.AsyncProducer, topic string, ch <-chan *BridgeMessage) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(m.Key),
				Value: sarama.ByteEncoder(m.Payload),
			}
			kafkaOut.Inc()
		}
	}
}

func newKafkaProducer(cfg *Config) sarama.AsyncProducer {
	c := sarama.NewConfig()
	c.Version = sarama.V3_3_0_0
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Producer.Compression = sarama.CompressionSnappy
	c.Producer.Flush.Frequency = 20 * time.Millisecond
	c.ClientID = cfg.KafkaClientID

	p, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, c)
	if err != nil {
		log.Fatalf("Kafka producer error: %v", err)
	}
	return p
}

func newMqttClient(cfg *Config, msgCh chan<- *BridgeMessage) mqtt.Client {
	opts := mqtt.NewClientOptions()
	for _, b := range cfg.MqttBrokers {
		opts.AddBroker(b)
	}
	opts.SetClientID(cfg.MqttClientID)
	opts.SetUsername(cfg.MqttUsername)
	opts.SetPassword(cfg.MqttPassword)
	opts.SetCleanSession(true)

	opts.SetDefaultPublishHandler(func(c mqtt.Client, m mqtt.Message) {
		payload := append([]byte(nil), m.Payload()...)
		key := deriveKey(payload)
		mqttIn.Inc()
		bufGauge.Set(float64(len(msgCh)))

		select {
		case msgCh <- &BridgeMessage{Key: key, Payload: payload}:
		default:
			log.Printf("Bridge buffer full → dropping message")
		}
	})

	return mqtt.NewClient(opts)
}
