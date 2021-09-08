package com.knoldus.service;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) {

        System.out.println("Creating kafka Producer.");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,Object> kafkaProducer = new KafkaProducer(properties);

        try  {
            for(int i=1;i<100;i++)
           kafkaProducer.send(new ProducerRecord("user",Integer.toString(1),"{Name:user"+i+"}"));

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Finished- closed Kafka Producer.");
            kafkaProducer.close();
        }
    }
}
