package org.acme;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import java.io.FileReader;

@ApplicationScoped
public class MqttKafkaBridge extends RouteBuilder {

    @ConfigProperty(name = "mqtt.tls.ca")
    String caPath;

    @ConfigProperty(name = "mqtt.tls.crt")
    String crtPath;

    @ConfigProperty(name = "mqtt.tls.key")
    String keyPath;

    @Singleton
    @Named("customSocketFactory")
    public SSLSocketFactory customSocketFactory() throws Exception {

        // 1. Load CA Certificate (TrustStore)
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        if (caPath != null && !caPath.isEmpty() && !"none".equals(caPath)) {
            try (var is = Files.newInputStream(Paths.get(caPath))) {
                Certificate caCert = cf.generateCertificate(is);
                trustStore.setCertificateEntry("ca-cert", caCert);
            }
        }

        // 2. Load Client Cert & Private Key (KeyStore)
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);

        if (crtPath != null && keyPath != null && !"none".equals(crtPath)) {
            // A. Load Certificate
            Certificate clientCert;
            try (var is = Files.newInputStream(Paths.get(crtPath))) {
                clientCert = cf.generateCertificate(is);
            }

            // B. Load Private Key using BouncyCastle (Handles PKCS#1 & PKCS#8)
            PrivateKey privateKey = null;
            try (PEMParser pemParser = new PEMParser(new FileReader(keyPath))) {
                Object object = pemParser.readObject();
                JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

                if (object instanceof PEMKeyPair) {
                    // This handles "BEGIN RSA PRIVATE KEY" (PKCS#1)
                    privateKey = converter.getKeyPair((PEMKeyPair) object).getPrivate();
                } else if (object instanceof org.bouncycastle.asn1.pkcs.PrivateKeyInfo) {
                    // This handles "BEGIN PRIVATE KEY" (PKCS#8)
                    privateKey = converter.getPrivateKey((org.bouncycastle.asn1.pkcs.PrivateKeyInfo) object);
                }
            }

            if (privateKey == null) {
                throw new RuntimeException("Could not parse Private Key from: " + keyPath);
            }

            // Set entry
            keyStore.setKeyEntry("client-key", privateKey, "password".toCharArray(), new Certificate[] { clientCert });
        }

        // 3. Initialize SSL Context
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "password".toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return sslContext.getSocketFactory();
    }

    @Override
    public void configure() throws Exception {
        String mqttBrokerUrl = "{{mqtt.broker.url}}";
        String mqttTopic = "{{mqtt.topic}}";
        String kafkaBrokers = "{{kafka.brokers}}";
        String kafkaTopic = "{{kafka.topic}}";

        // SCALING UPDATE: Append Random UUID to Client ID.
        // This ensures every replica has a unique ID, preventing broker disconnects
        // (fighting).
        String clientId = "{{bridge.id}}-" + UUID.randomUUID().toString().substring(0, 8);

        onException(Exception.class)
                .log("ERROR processing message: ${exception.message}")
                .handled(true);

        from("paho-mqtt5:" + mqttTopic + "?brokerUrl=" + mqttBrokerUrl +
                "&clientId=" + clientId +
                "&qos=1" +
                // SCALING UPDATE: cleanStart=true
                // Since Client IDs are now random/ephemeral, we must start clean to avoid
                // leaving thousands of dead sessions on the broker during restarts.
                // (Note: To load balance, use a Shared Subscription topic: $share/group/topic)
                "&cleanStart=true" +
                "&automaticReconnect=true" +
                "&socketFactory=#customSocketFactory")

                .routeId("mqtt-to-kafka-route")

                .to("micrometer:counter:bridge_messages_in")

                .process(exchange -> {
                    String topic = exchange.getIn().getHeader(PahoMqtt5Constants.MQTT_TOPIC, String.class);
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

                .to("kafka:" + kafkaTopic + "?brokers=" + kafkaBrokers +
                        "&requestRequiredAcks=all" +
                        "&compressionCodec=lz4")

                .to("micrometer:counter:bridge_messages_out");
    }
}