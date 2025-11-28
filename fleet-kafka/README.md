# FLEET KAFKA

This component installs all kafka related components:

- AMQ streams operator
- Kafka Brokers
- Kafka topic
- Kafka Bridge MQTT
- Fleet Aggregator
- Camel bridge

### Use case 1 - Custom Go bridge

Configure: 
```
consumer: aggregator
bridge:
  type: go
```

Message flow: 

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

### Use case 2 - Camel bridge

Configure:
```
bridge:
  type: camel
consumer: aggregator  
```

Message flow:
```
[Trucks → Artemis MQTT (TLS/mTLS)]
            ↓
[MQTT → Kafka Bridge (CAMEL on quarkus)]
            ↓
 [Kafka Topic (48–96 partitions)]
            ↓
[Aggregator (Go) → Prometheus]
            ↓
        [Grafana]
 ```   

### Use case 3 - Kafka Connect

Configure:
```
bridge:
  type: connect
consumer: aggregator    

```

Message flow:

```
[Trucks → Artemis MQTT (TLS/mTLS)]
            ↓
[MQTT → Kafka Connect using Streams connector]
            ↓
 [Kafka Topic (48–96 partitions)]
            ↓
[Aggregator (Go) → Prometheus]
            ↓
        [Grafana]
 ```   



### Use case 4 - AMQ Broker- > Telegraf ( No Kafka)

```
consumer: telegraf     
```

Message flow: 
```
[Trucks → Artemis MQTT (TLS/mTLS)]
            ↓
[Telegraf → Prometheus]
            ↓
        [Grafana]
 ```  