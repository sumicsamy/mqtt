package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func main() {

	// ---------------------------------------------------------
	//  ENV CONFIG
	// ---------------------------------------------------------
	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		log.Fatal("Missing env: BROKER_URL")
	}

	topic := os.Getenv("MQTT_TOPIC")
	if topic == "" {
		log.Fatal("Missing env: MQTT_TOPIC")
	}

	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		clientID = "fast-mqtt-consumer"
	}

	// ---------------------------------------------------------
	//  CONTEXT + SIGNAL HANDLER
	// ---------------------------------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	parsedURL, err := url.Parse(brokerURL)
	if err != nil {
		log.Fatalf("Invalid BROKER_URL: %v", err)
	}

	// ---------------------------------------------------------
	//  LOAD mTLS MATERIALS
	// ---------------------------------------------------------
	caBytes, err := os.ReadFile("/certs/ca.crt")
	if err != nil {
		log.Fatalf("Failed to load CA cert: %v", err)
	}

	clientCert, err := tls.LoadX509KeyPair("/certs/tls.crt", "/certs/tls.key")
	if err != nil {
		log.Fatalf("Failed to load client cert/key: %v", err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caBytes)

	tlsCfg := &tls.Config{
		RootCAs:            certPool,
		Certificates:       []tls.Certificate{clientCert},
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // required for many Artemis TLS setups
	}

	// ---------------------------------------------------------
	//  THROUGHPUT COUNTER (lock-free)
	// ---------------------------------------------------------
	var msgCount uint64

	// ---------------------------------------------------------
	//  AUTOPAHO CONFIG
	// ---------------------------------------------------------
	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{parsedURL},
		TlsCfg:     tlsCfg,
		KeepAlive:  30,

		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         300,

		ClientConfig: paho.ClientConfig{
			ClientID: clientID,

			// FAST PATH: discard + atomic incr
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					atomic.AddUint64(&msgCount, 1)
					return true, nil
				},
			},
		},

		OnConnectionUp: func(cm *autopaho.ConnectionManager, ack *paho.Connack) {
			log.Println("Connected. Subscribing:", topic)
			_, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: topic, QoS: 1},
				},
			})
			if err != nil {
				log.Printf("Subscribe failed: %v", err)
			}
		},

		OnConnectError: func(err error) {
			log.Printf("Connect error: %v", err)
		},
	}

	// ---------------------------------------------------------
	//  CONNECT
	// ---------------------------------------------------------
	client, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create connection manager: %v", err)
	}

	if err := client.AwaitConnection(ctx); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}

	// ---------------------------------------------------------
	//  RATE PRINTER
	// ---------------------------------------------------------
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			n := atomic.SwapUint64(&msgCount, 0)
			fmt.Printf("Rate: %d msg/s\n", n)
		}
	}()

	// ---------------------------------------------------------
	//  BLOCK
	// ---------------------------------------------------------
	<-ctx.Done()
	fmt.Println("Shutdown...")
	client.Disconnect(context.Background())
}
