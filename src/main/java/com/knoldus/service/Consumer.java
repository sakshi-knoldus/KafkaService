package com.knoldus.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Collections.singletonList("user"));
        try {
            System.out.println("Consumer is Started");
            while (true) {
                ConsumerRecords<String, Object> records = kafkaConsumer.poll(Duration.ofMillis(10000));
                System.out.println(records.toString());
                if (records.count() == 0) {
                    System.out.println("No records");
                    break;
                }
                for (ConsumerRecord<String, Object> record : records) {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            System.out.println("Closing the kafka consumer");
            kafkaConsumer.close();
        }
    }
}
