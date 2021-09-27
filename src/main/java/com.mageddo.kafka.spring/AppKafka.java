package com.mageddo.kafka.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;

@EnableKafka
@EnableScheduling
@SpringBootApplication
public class AppKafka {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final KafkaTemplate<String, String> kafkaTemplate;

    public AppKafka(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String... args) {
        SpringApplication.run(AppKafka.class, args);
    }

    @Scheduled(fixedDelay = 100 * 100)
    void produceKafka() {
        for (int a = 0; a < 10; a++) {
            final String value = "Viagem Orlando: " + UUID.randomUUID();

            this.kafkaTemplate.send("compras", value);

            logger.info("Producer n° " + a + " ==> " + value);
        }
    }

    @KafkaListener(topics = "compras", groupId = "comprar-1")
    void consumeKafka(String consumer) {
        logger.info("Consumer ==> " + consumer);
    }

}
