package org.acme;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants;

@ApplicationScoped
public class MqttKafkaBridge extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        // -------------------------------------------------------------
        // Configuration
        // -------------------------------------------------------------
        String mqttBrokerUrl = "{{mqtt.broker.url}}";
        String mqttTopic     = "{{mqtt.topic}}";
        
        String kafkaBrokers  = "{{kafka.brokers}}";
        String kafkaTopic    = "{{kafka.topic}}";
        String clientId      = "{{bridge.id}}";

        // -------------------------------------------------------------
        // Error Handling
        // -------------------------------------------------------------
        onException(Exception.class)
            .log("ERROR processing message: ${exception.message}")
            .handled(true);

        // -------------------------------------------------------------
        // The Route
        // -------------------------------------------------------------
        from("paho-mqtt5:" + mqttTopic + "?brokerUrl=" + mqttBrokerUrl + 
             "&clientId=" + clientId + 
             "&qos=1" +                  // MQTT QoS 1 (At Least Once)
             "&cleanStart=false" +       
             "&automaticReconnect=true")
             
            .routeId("mqtt-to-kafka-route")
            
            // 1. Metric: Count incoming messages
            .to("micrometer:counter:bridge_messages_in")

            // 2. Processor: Derive Kafka Partition Key from MQTT Topic
            .process(exchange -> {
                String topic = exchange.getIn().getHeader(PahoMqtt5Constants.MQTT_TOPIC, String.class);
                
                // Topic: mine/fleet/<region>/truck/<truckID>/<subtopic>
                // Index:  0     1      2       3       4         5
                
                if (topic != null) {
                    String[] parts = topic.split("/");
                    if (parts.length >= 5) {
                        String key = parts[2] + ":" + parts[4];
                        exchange.getIn().setHeader(KafkaConstants.KEY, key);
                    } else {
                        exchange.getIn().setHeader(KafkaConstants.KEY, "unknown:unknown");
                    }
                }
            })

            // 3. Send to Kafka
            // HERE IS WHERE YOU SET IT:
            // "requestRequiredAcks=all" enables QoS 1 behavior (wait for replicas).
            // This setting is required for Idempotence to work without crashing.
            .to("kafka:" + kafkaTopic + "?brokers=" + kafkaBrokers + 
                "&requestRequiredAcks=all" +   
                "&compressionCodec=lz4")    

            // 4. Metric: Count outgoing messages
            .to("micrometer:counter:bridge_messages_out");
    }
}