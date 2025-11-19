package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
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
	MqttSharedGroup string // $share/<group>/...

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
		MqttBrokers:     strings.Split(getenv("MQTT_BROKERS", "mqtt://mqtt:1883"), ","),
		MqttTopicFilter: getenv("MQTT_TOPIC", "mine/fleet/+/truck/+/telemetry"),
		MqttClientID:    getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge"),
		MqttUsername:    getenv("MQTT_USERNAME", ""),
		MqttPassword:    getenv("MQTT_PASSWORD", ""),
		MqttSharedGroup: getenv("MQTT_SHARED_GROUP", "bridge"),

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

/************** Kafka **************/

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
		log.Fatalf("[KAFKA] Producer init error: %v", err)
	}
	return p
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

/************** MQTT v5 with autopaho **************/

func newMQTTConnection(ctx context.Context, cfg *Config, msgCh chan<- *BridgeMessage) *autopaho.ConnectionManager {
	if len(cfg.MqttBrokers) == 0 || cfg.MqttBrokers[0] == "" {
		log.Fatalf("[MQTT] MQTT_BROKERS is empty or invalid")
	}

	var urls []*url.URL
	for _, raw := range cfg.MqttBrokers {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			log.Fatalf("[MQTT] invalid broker URL %q: %v", raw, err)
		}
		urls = append(urls, u)
	}

	// Router: handles all incoming publishes
	router := paho.NewStandardRouterWithDefault(func(p *paho.Publish) {
		payload := append([]byte(nil), p.Payload...)
		key := deriveKey(payload)

		mqttIn.Inc()
		bufGauge.Set(float64(len(msgCh)))

		select {
		case msgCh <- &BridgeMessage{Key: key, Payload: payload}:
		default:
			log.Printf("[MQTT] Buffer full → dropping message (topic=%s)", p.Topic)
		}
	})

	sharedTopic := fmt.Sprintf("$share/%s/%s", cfg.MqttSharedGroup, cfg.MqttTopicFilter)

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    urls,
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		// Keep session alive for a while to survive short outages
		SessionExpiryInterval: 600,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Printf("[MQTT] connection up, subscribing to %s (QoS=%d)", sharedTopic, cfg.MqttQos)

			_, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{
						Topic: sharedTopic,
						QoS:   cfg.MqttQos,
					},
				},
			})
			if err != nil {
				log.Printf("[MQTT] subscribe failed: %v (no messages will be received until this succeeds)", err)
				return
			}
			log.Printf("[MQTT] subscription to %s established", sharedTopic)
		},
		OnConnectError: func(err error) {
			log.Printf("[MQTT] error whilst attempting connection: %v", err)
		},
		// Username / password (MQTT 5 CONNECT properties)
		ConnectUsername: cfg.MqttUsername,
		ConnectPassword: []byte(cfg.MqttPassword),

		// Base MQTT v5 client config
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.MqttClientID,
			Router:   router,
			OnClientError: func(err error) {
				log.Printf("[MQTT] client error: %v", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Printf("[MQTT] server requested disconnect: %s", d.Properties.ReasonString)
				} else {
					log.Printf("[MQTT] server requested disconnect; reason code: %d", d.ReasonCode)
				}
			},
		},
	}

	log.Printf("[MQTT] brokers: %v", cfg.MqttBrokers)
	log.Printf("[MQTT] topic filter: %s", cfg.MqttTopicFilter)
	log.Printf("[MQTT] shared subscription: %s", sharedTopic)
	log.Printf("[MQTT] clientID: %s", cfg.MqttClientID)

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		log.Fatalf("[MQTT] failed to create connection manager: %v", err)
	}

	// Wait for first successful connection
	if err := cm.AwaitConnection(ctx); err != nil {
		log.Fatalf("[MQTT] initial connection failed: %v", err)
	}

	return cm
}

/************** Main **************/

func main() {
	cfg := loadConfig()

	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		log.Fatalf("[KAFKA] KAFKA_BROKERS is empty or invalid")
	}

	log.Printf("[KAFKA] Brokers: %v", cfg.KafkaBrokers)

	msgCh := make(chan *BridgeMessage, cfg.MessageBuffer)

	// Kafka producer + error logger
	producer := newKafkaProducer(cfg)
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			kafkaErr.Inc()
			log.Printf("[KAFKA] Error: %v", err)
		}
	}()

	// Context + signal handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Worker pool
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, producer, cfg.KafkaTopic, msgCh)
	}

	// MQTT v5 connection (AutoPaho handles reconnects)
	cm := newMQTTConnection(ctx, cfg, msgCh)

	// Prometheus endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("[HTTP] Serving metrics on :%s/metrics", cfg.PromPort)
		if err := http.ListenAndServe(":"+cfg.PromPort, nil); err != nil {
			log.Fatalf("[HTTP] ListenAndServe error: %v", err)
		}
	}()

	// Wait for termination signal
	<-ctx.Done()
	log.Printf("[MAIN] signal received → shutting down")

	// Ask MQTT manager to disconnect cleanly
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cm.Disconnect(shutdownCtx); err != nil {
		log.Printf("[MQTT] disconnect error: %v", err)
	}
	<-cm.Done() // wait for MQTT to finish

	close(msgCh)
	wg.Wait()

	log.Printf("[MAIN] shutdown complete")
}
