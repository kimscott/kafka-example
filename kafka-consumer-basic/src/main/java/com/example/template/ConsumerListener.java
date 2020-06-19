package com.example.template;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);

//    @KafkaListener(topics = "first_topic")
    public void listen_by_line(@Payload String message, Consumer<?, ?> consumer, ConsumerRecord<?, ?> consumerRecord) {
        logger.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
        logger.info("Partition: " + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
    }

    /**
     * 특정 파티션의 데이터를 가져오는 방법
     */
//    @KafkaListener(topicPartitions
//            = @TopicPartition(topic = "first_topic", partitions = { "0" })
//            , groupId = "bar1")
    public void listen_by_partition(@Payload String message, Consumer<?, ?> consumer, ConsumerRecord<?, ?> consumerRecord) {
        logger.info("topic name = first_topic, partition = 0  ");
        logger.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
        logger.info("Partition: " + consumerRecord.partition() + ", Offset:" + consumerRecord.offset());
    }

    /**
     * 특정 파티션의 특정 오프셋으로 부터 토픽을 시작하는 방법
     */
//    @KafkaListener(topicPartitions
//            = @TopicPartition(topic = "first_topic",
//            partitionOffsets = {
//                    @PartitionOffset(partition = "0" , initialOffset = "70" )
//            }  )
//            , groupId = "bar2")
    public void listenWithSpecificOffset(@Payload String message, ConsumerRecord<?, ?> consumerRecord) {
        logger.info("listenWithSpecificOffset Key='{}' Value='{}' partition='{}' Offset='{}'", consumerRecord.key() , message, consumerRecord.partition(), consumerRecord.offset());
    }
}
