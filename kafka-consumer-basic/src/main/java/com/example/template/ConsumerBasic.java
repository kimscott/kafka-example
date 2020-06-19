package com.example.template;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@Service
public class ConsumerBasic {

    Logger logger = LoggerFactory.getLogger(ConsumerBasic.class.getName());

    @Autowired
    ConsumerConfiguration consumerConfiguration;

    public void listen_kafka_lib() {

        String topic = "first_topic";

        // consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfiguration.consumerConfigs());

        // 메세지 수신
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }

        }

    }
}
