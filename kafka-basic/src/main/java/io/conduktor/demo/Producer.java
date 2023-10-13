package io.conduktor.demo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("=== Producer ===");

        // create Producer properties
        Properties properties = new Properties();

        // Conduktor properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "precious-parakeet-11078-us1-kafka.upstash.io:9092");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJlY2lvdXMtcGFyYWtlZXQtMTEwNzgkmzAOMnpcdVSr0OPyEqVKB6UgGYNVRm4\" password=\"N2E0MjBjZTYtMDhlYy00MDNkLWE0MGMtOWM0NDBjZDhlNDMw\";");

        // Producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Producer with Callback
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "record " + i;

                // create a Producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // execute every time a record successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata: " +
                                "Key: " + key + " | " +
                                "Topic: " + recordMetadata.topic() + " | " +
                                "Partition: " + recordMetadata.partition() + " | " +
                                "Offset: " + recordMetadata.offset() + " | " +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}