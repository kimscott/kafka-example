package com.example.template;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SimpleJsonMessageProducer {

    public static void main(String[] args) {


        Map<String, Object> props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int total = 1000;
        String topicName = "input_topic";
        Random random = new Random();
        for(int i=0; i <= total; i++) {
            int qty = random.nextInt(1000 ) + 1;
            JSONObject data = new JSONObject();
            data.put("orderId", i);
            data.put("product", "tv");
            data.put("qty", qty);

            System.out.println(data.toString());
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topicName , data.toString());

            producer.send(record);
        }

        producer.flush();
        producer.close();
    }
}
