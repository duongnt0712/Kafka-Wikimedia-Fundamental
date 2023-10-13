package io.conduktor.demo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("=== Consumer ===");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Consumer properties
        Properties properties = new Properties();
        // Conduktor properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "precious-parakeet-11078-us1-kafka.upstash.io:9092");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJlY2lvdXMtcGFyYWtlZXQtMTEwNzgkmzAOMnpcdVSr0OPyEqVKB6UgGYNVRm4\" password=\"N2E0MjBjZTYtMDhlYy00MDNkLWE0MGMtOWM0NDBjZDhlNDMw\";");
        // Consumer properties
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown. Let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            // poll for data
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down.");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer: ", e);
        } finally {
            // close the consumer, also commit offsets
            consumer.close();
            log.info("The consumer is now gracefully shut down");

        }
    }
}