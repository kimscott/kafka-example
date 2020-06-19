package com.example.template;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducerKey {

    Logger logger = LoggerFactory.getLogger(ProducerKey.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ProducerConfiguration producerConfiguration;

    public void send_kafka_lib(){

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfiguration.producerConfigs());

        for (int i=0; i<10; i++ ) {

            String key = "id_" + Integer.toString(i);
            String value = "value_" + Integer.toString(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", key,value);

            producer.send(record);
        }

        producer.flush();
        producer.close();
    }


    public void send_springboot_lib(){

        for (int i=10; i<20; i++ ) {

            String key = "id_" + Integer.toString(i);
            String value = "value_" + Integer.toString(i);

            ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send("first_topic" ,key, value);
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

            kafkaTemplate.flush();

        }

    }

}
