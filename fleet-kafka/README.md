# FLEET KAFKA

This component installs all kafka related components:

- AMQ streams operator
- Kafka Brokers
- Kafka topic
- Kafka Bridge MQTT
- Fleet Aggregator

```
[Trucks → Artemis MQTT (TLS/mTLS)]
            ↓
   [MQTT → Kafka Bridge (Go)]
            ↓
 [Kafka Topic (48–96 partitions)]
            ↓
[Aggregator (Go) → Prometheus]
            ↓
        [Grafana]
 ```           