package com.example.template;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducerBasic {

    Logger logger = LoggerFactory.getLogger(ProducerBasic.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ProducerConfiguration producerConfiguration;

    public void send_kafka_lib(){

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfiguration.producerConfigs());

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "this message sand from application send_kafka_lib");

        producer.send(record);

    }


    public void send_kafka_lib_callback(){

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfiguration.producerConfigs());

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "this message sand from application send_kafka_lib_callback");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });

    }

    public void send_springboot_lib(){

        kafkaTemplate.send("first_topic" , "send_springboot_lib1");

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "send_springboot_lib2");
        kafkaTemplate.send(record);
    }

    public void send_springboot_lib_callback(){

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "send_springboot_lib_callback");

        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(record);
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                logger.info("Received send_springboot_lib_callback new metadata. \n" +
                        "Topic:" + result.getRecordMetadata().topic() + "\n" +
                        "Partition: " + result.getRecordMetadata().partition() + "\n" +
                        "Offset: " + result.getRecordMetadata().offset() + "\n" +
                        "Timestamp: " + result.getRecordMetadata().timestamp());
            }

            @Override
            public void onFailure(Throwable ex) {
            }
        });
    }
}
