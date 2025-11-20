package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	paho "github.com/eclipse/paho.golang/paho"
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

	MqttTLSCA   string
	MqttTLSCert string
	MqttTLSKey  string

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
		MqttBrokers:     strings.Split(getenv("MQTT_BROKERS", "tls://mqtt-lb.mqtt-broker.svc.cluster.local:8883"), ","),
		MqttTopicFilter: getenv("MQTT_TOPIC", "mine/fleet/+/truck/+/telemetry"),
		MqttClientID:    getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge"),
		MqttUsername:    getenv("MQTT_USERNAME", ""),
		MqttPassword:    getenv("MQTT_PASSWORD", ""),
		MqttSharedGroup: getenv("MQTT_SHARED_GROUP", "bridge"),

		MqttTLSCA:   getenv("MQTT_TLS_CA", "/certs/ca.crt"),
		MqttTLSCert: getenv("MQTT_TLS_CERT", "/certs/tls.crt"),
		MqttTLSKey:  getenv("MQTT_TLS_KEY", "/certs/tls.key"),

		KafkaBrokers:  strings.Split(getenv("KAFKA_BROKERS", "fleet-kafka-kafka-bootstrap:9092"), ","),
		KafkaTopic:    getenv("KAFKA_TOPIC", "truck-telemetry"),
		KafkaClientID: getenv("KAFKA_CLIENT_ID", "mqtt-kafka-bridge"),

		WorkerCount:   mustInt("WORKER_COUNT", "32"),
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

/************** TLS Helper **************/

func loadTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	rootCAs := x509.NewCertPool()

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	if ok := rootCAs.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("append CA cert failed")
	}

	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert/key: %w", err)
	}

	return &tls.Config{
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

/************** Kafka worker **************/

func worker(ctx context.Context, wg *sync.WaitGroup, producer sarama.AsyncProducer, topic string, ch <-chan *BridgeMessage) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				log.Printf("error publishing to kafka %s", string(m.Payload))
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

	// Throughput settings
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = sarama.NoResponse // <-- BIGGEST BOOST
	c.Producer.Compression = sarama.CompressionLZ4

	// Batch aggressively
	c.Producer.Flush.Frequency = 1 * time.Millisecond
	c.Producer.Flush.Messages = 1000
	c.Producer.Flush.Bytes = 2 * 1024 * 1024

	// Channels
	c.ChannelBufferSize = 4096

	p, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, c)
	if err != nil {
		log.Fatalf("[KAFKA] Producer init error: %v", err)
	}
	return p
}

/************** MQTT v5 (autopaho) **************/

func buildAutopahoConfig(ctx context.Context, cfg *Config, msgCh chan<- *BridgeMessage) (*autopaho.ConnectionManager, error) {
	// Parse broker URLs
	var urls []*url.URL
	for _, raw := range cfg.MqttBrokers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse MQTT broker URL %q: %w", raw, err)
		}
		urls = append(urls, u)
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("no valid MQTT broker URLs")
	}

	tlsCfg, err := loadTLSConfig(cfg.MqttTLSCA, cfg.MqttTLSCert, cfg.MqttTLSKey)
	if err != nil {
		return nil, fmt.Errorf("TLS config error: %w", err)
	}

	sharedTopic := fmt.Sprintf("$share/%s/%s", cfg.MqttSharedGroup, cfg.MqttTopicFilter)

	clientCfg := autopaho.ClientConfig{
		ServerUrls:                    urls,
		TlsCfg:                        tlsCfg,
		KeepAlive:                     30,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         600,

		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			log.Printf("[MQTT] connection up, subscribing to shared topic: %s", sharedTopic)

			_, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{
						Topic: sharedTopic,
						QoS:   cfg.MqttQos,
					},
				},
			})
			if err != nil {
				log.Printf("[MQTT] subscribe failed: %v", err)
				return
			}
			log.Printf("[MQTT] subscribed to %s", sharedTopic)
		},

		OnConnectionDown: func() bool {
			log.Printf("[MQTT] connection down, will retry")
			return true // keep retrying
		},

		OnConnectError: func(err error) {
			log.Printf("[MQTT] error whilst attempting connection: %v", err)
		},

		// Base paho client config used for each connection
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.MqttClientID,
			// Incoming messages handler (added below via AddOnPublishReceived)
			OnPublishReceived: []func(m paho.PublishReceived) (bool, error){
				func(m paho.PublishReceived) (bool, error) {
					payload := m.Packet.Payload
					mqttIn.Inc()

					key := deriveKey(payload)
					// log.Printf("[MQTT5] OnPublishReceived with payload %s key", string(key))
					select {
					case msgCh <- &BridgeMessage{Key: key, Payload: payload}:
					default:
						log.Printf("[MQTT5] buffer full, dropping")
					}
					return true, nil
				},
			},
		},
	}

	// Create connection manager (starts background connect/reconnect loop)
	cm, err := autopaho.NewConnection(ctx, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("NewConnection: %w", err)
	}

	// // Handler for incoming PUBLISH packets
	// cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {

	// 	payload := append([]byte(nil), pr.Packet.Payload...)
	// 	key := deriveKey(payload)
	// 	log.Printf("[MQTT] additional OnPublishReceived handler called %s", key)
	// 	mqttIn.Inc()
	// 	bufGauge.Set(float64(len(msgCh)))

	// 	select {
	// 	case msgCh <- &BridgeMessage{Key: key, Payload: payload}:
	// 	default:
	// 		log.Printf("[MQTT] buffer full → dropping message")
	// 	}

	// 	return true, nil
	// })

	return cm, nil
}

/************** Main **************/

func main() {
	cfg := loadConfig()

	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		log.Fatalf("[KAFKA] KAFKA_BROKERS is empty or invalid")
	}

	log.Printf("[KAFKA] Brokers: %v", cfg.KafkaBrokers)
	log.Printf("[MQTT] Brokers: %v", cfg.MqttBrokers)
	log.Printf("[MQTT] Topic filter: %s", cfg.MqttTopicFilter)
	log.Printf("[MQTT] Shared group: %s", cfg.MqttSharedGroup)

	msgCh := make(chan *BridgeMessage, cfg.MessageBuffer)

	// Kafka producer
	producer := newKafkaProducer(cfg)
	defer producer.Close()

	// Background logging for Kafka async errors
	go func() {
		for err := range producer.Errors() {
			kafkaErr.Inc()
			log.Printf("[KAFKA] error: %v", err)
		}
	}()

	// Worker pool
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, producer, cfg.KafkaTopic, msgCh)
	}

	// MQTT v5 connection manager
	cm, err := buildAutopahoConfig(ctx, cfg, msgCh)
	if err != nil {
		log.Fatalf("[MQTT] config/init error: %v", err)
	}

	// Wait for connection to come up once
	if err := cm.AwaitConnection(ctx); err != nil {
		log.Fatalf("[MQTT] AwaitConnection error: %v", err)
	}
	log.Printf("[MQTT] initial connection established")

	// Prometheus HTTP
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("[HTTP] Serving metrics on :%s/metrics", cfg.PromPort)
		if err := http.ListenAndServe(":"+cfg.PromPort, nil); err != nil {
			log.Fatalf("[HTTP] ListenAndServe error: %v", err)
		}
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("[MAIN] Shutting down...")
	cancel()

	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	if err := cm.Disconnect(shutdownCtx); err != nil {
		log.Printf("[MQTT] disconnect error: %v", err)
	}
	close(msgCh)
	wg.Wait()
	log.Printf("[MAIN] Shutdown complete")
}
