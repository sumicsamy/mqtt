# MQTT KAFKA BRIDGE

custom bridge to push messages from amq broker to kafka broker

```
docker build -t quay.io/your-org/mqtt-kafka-bridge:latest .
docker push quay.io/your-org/mqtt-kafka-bridge:latest
```

A custom build golang bridge for pulling from broker and pushing to kafka.