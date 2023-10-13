package io.conduktor.demo;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class WikimediaChangesHandler implements EventHandler {

    KafkaTemplate<String, String> kafkaTemplate;
    String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // nothing here
    }

    @Override
    public void onClosed() throws Exception {
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // asynchronous
        kafkaTemplate.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {
        // nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in Stream Reading", throwable);
    }
}