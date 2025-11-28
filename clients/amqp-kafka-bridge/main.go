package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// CHANGED: Switched from RabbitMQ (0.9.1) to Azure/go-amqp (AMQP 1.0) for Red Hat AMQ 7
	"github.com/Azure/go-amqp"
)

type Config struct {
	// AMQP Settings
	AmqpURL      string
	AmqpAddress  string // Queue or Topic name (Source)
	AmqpPrefetch int    // Link Credit

	// TLS
	AmqpTLSCA   string
	AmqpTLSCert string
	AmqpTLSKey  string

	// Kafka Settings
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaClientID string

	// General
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
	return &Config{
		// AMQ 7 Default Port is 5672 (non-TLS) or 5671 (TLS)
		AmqpURL:      getenv("AMQP_URL", "amqps://admin:admin@amq-broker-service:5671/"),
		AmqpAddress:  getenv("AMQP_ADDRESS", "telemetry_bridge_queue"),
		AmqpPrefetch: mustInt("AMQP_PREFETCH", "500"),

		AmqpTLSCA:   getenv("AMQP_TLS_CA", "/certs/ca.crt"),
		AmqpTLSCert: getenv("AMQP_TLS_CERT", "/certs/tls.crt"),
		AmqpTLSKey:  getenv("AMQP_TLS_KEY", "/certs/tls.key"),

		KafkaBrokers:  strings.Split(getenv("KAFKA_BROKERS", "fleet-kafka-kafka-bootstrap:9092"), ","),
		KafkaTopic:    getenv("KAFKA_TOPIC", "truck-telemetry"),
		KafkaClientID: getenv("KAFKA_CLIENT_ID", "amqp-kafka-bridge"),

		WorkerCount:   mustInt("WORKER_COUNT", "32"),
		MessageBuffer: mustInt("MESSAGE_BUFFER", "100000"),
		PromPort:      getenv("PROM_PORT", "8080"),
	}
}

/************** Prometheus Metrics **************/
var (
	amqpIn = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "amqp_messages_in_total",
		Help: "AMQP messages received",
	})

	kafkaOut = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_out_total",
		Help: "Messages forwarded to Kafka",
	})

	kafkaErr = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_error_total",
		Help: "Kafka produce errors",
	})
)

func init() {
	prometheus.MustRegister(amqpIn, kafkaOut, kafkaErr)
}

/**************************************************/

type BridgeMessage struct {
	Key     string
	Payload []byte
}

// In AMQP 1.0 (AMQ 7), the topic is often in the 'Subject' property.
// Separators might be '.' or '/' depending on broker config.
func deriveKeyFromSubject(subject string) string {
	if subject == "" {
		return "unknown:unknown"
	}
	// Normalize separators: AMQ 7 might use dots, MQTT uses slashes
	s := strings.ReplaceAll(subject, "/", ".")
	parts := strings.Split(s, ".")

	// expected: mine.fleet.<region>.truck.<truckID>.<subtopic>
	if len(parts) >= 5 {
		region := parts[2]
		truck := parts[4]
		return region + ":" + truck
	}
	return "unknown:unknown"
}

/************** TLS Helper **************/

func loadTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	// If CA path doesn't exist, return nil to allow non-TLS connection attempt or system defaults
	if _, err := os.Stat(caPath); os.IsNotExist(err) {
		return nil, nil
	}

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

/************** Kafka **************/

func newKafkaProducer(cfg *Config) sarama.AsyncProducer {
	c := sarama.NewConfig()
	c.Version = sarama.V3_3_0_0
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = sarama.NoResponse
	c.Producer.Compression = sarama.CompressionLZ4
	c.Producer.Flush.Frequency = 5 * time.Millisecond
	c.Producer.Flush.Messages = 200

	p, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, c)
	if err != nil {
		log.Fatalf("[KAFKA] Producer init error: %v", err)
	}
	return p
}

/************** AMQP 1.0 Consumer Loop **************/

func startAMQPConsumer(ctx context.Context, cfg *Config, msgCh chan<- *BridgeMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := runSession(ctx, cfg, msgCh); err != nil {
				log.Printf("[AMQP] Session ended: %v. Retrying in 5s...", err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// runSession handles the connection lifecycle for one session
func runSession(ctx context.Context, cfg *Config, msgCh chan<- *BridgeMessage) error {
	// 1. Configure TLS
	tlsConfig, err := loadTLSConfig(cfg.AmqpTLSCA, cfg.AmqpTLSCert, cfg.AmqpTLSKey)
	if err != nil {
		return fmt.Errorf("TLS config: %w", err)
	}

	connOpts := &amqp.ConnOptions{
		TLSConfig: tlsConfig,
		// AMQ 7 (Artemis) usually requires SASL Plain or Anonymous
		SASLType: amqp.SASLTypeAnonymous(),
	}

	// 2. Connect
	log.Printf("[AMQP] Dialing %s", cfg.AmqpURL)
	conn, err := amqp.Dial(ctx, cfg.AmqpURL, connOpts)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// 3. Open Session
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}

	// 4. Create Receiver (Consumer)
	// 'Credit' here replaces 'Prefetch'. It controls how many messages the broker sends before waiting.
	rcvOpts := &amqp.ReceiverOptions{
		Credit: int32(cfg.AmqpPrefetch),
	}

	receiver, err := session.NewReceiver(ctx, cfg.AmqpAddress, rcvOpts)
	if err != nil {
		return fmt.Errorf("new receiver for address %q: %w", cfg.AmqpAddress, err)
	}
	defer receiver.Close(ctx)

	log.Printf("[AMQP] Connected. Consuming from: %s", cfg.AmqpAddress)

	// 5. Consumption Loop
	for {
		// Block until message arrives or context cancels
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}

		amqpIn.Inc()

		// Extract Subject (Topic)
		subject := ""
		if msg.Properties != nil && msg.Properties.Subject != nil {
			subject = *msg.Properties.Subject
		}

		// If Subject is empty (some configs), check Annotations or fallback to "To"
		if subject == "" && msg.Properties != nil && msg.Properties.To != nil {
			subject = *msg.Properties.To
		}

		key := deriveKeyFromSubject(subject)

		// Send to Internal Buffer
		select {
		case msgCh <- &BridgeMessage{Key: key, Payload: msg.GetData()}:
			// Success: Acknowledge message to AMQ Broker
			if err := receiver.AcceptMessage(ctx, msg); err != nil {
				log.Printf("[AMQP] Failed to accept message: %v", err)
			}
		default:
			// Buffer full: Reject or Release message so broker redelivers later
			// This provides backpressure to the broker.
			log.Printf("[AMQP] Buffer full, releasing message")
			receiver.ReleaseMessage(ctx, msg)
		}
	}
}

/************** Worker **************/

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

/************** Main **************/

func main() {
	cfg := loadConfig()

	log.Printf("[KAFKA] Brokers: %v", cfg.KafkaBrokers)
	log.Printf("[AMQP] URL: %s (Addr: %s)", cfg.AmqpURL, cfg.AmqpAddress)

	msgCh := make(chan *BridgeMessage, cfg.MessageBuffer)

	// Kafka Setup
	producer := newKafkaProducer(cfg)
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			kafkaErr.Inc()
			log.Printf("[KAFKA] error: %v", err)
		}
	}()

	// Workers
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, producer, cfg.KafkaTopic, msgCh)
	}

	// AMQP Consumer (Runs in background)
	go startAMQPConsumer(ctx, cfg, msgCh)

	// Metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("[HTTP] Serving metrics on :%s/metrics", cfg.PromPort)
		http.ListenAndServe(":"+cfg.PromPort, nil)
	}()

	// Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("[MAIN] Shutting down...")
	cancel()
	close(msgCh)
	wg.Wait()
	log.Printf("[MAIN] Shutdown complete")
}
