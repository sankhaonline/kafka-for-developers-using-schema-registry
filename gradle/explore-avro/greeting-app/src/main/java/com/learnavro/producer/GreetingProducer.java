package com.learnavro.producer;

import com.learnavro.Greeting;
import com.learnavro.consumer.GreetingConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class GreetingProducer {
  private static final String GREETING_TOPIC = "greeting";

  public static void main(String... args)
      throws IOException, InterruptedException, ExecutionException {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    var producer = new KafkaProducer<String, byte[]>(props);

    var greeting = buildGreeting("Hello, Schema Registry!");

    var value = greeting.toByteBuffer().array();

    var producerRecord = new ProducerRecord<String, byte[]>(GREETING_TOPIC, value);
    var recordMetaData = producer.send(producerRecord).get();
    log.info("recordMetaData : {}", recordMetaData);
  }

  private static Greeting buildGreeting(final String message) {

    return Greeting.newBuilder()
        .setGreeting(message)
        .setId(UUID.randomUUID())
        // .setCreatedDateTimeLocal(LocalDateTime.now()) // LocalDateTime
        .setCreatedDateTime(Instant.now()) // UTC dateTime
        .setCreatedDate(LocalDate.now()) // LocalDate
        .setCost(BigDecimal.valueOf(3.999)) // 123.45 has a precision of 5 and a scale of 2.
        .build();
  }
}
