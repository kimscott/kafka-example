package com.example;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFilterByEvent {
    /**
     * input_topic 토픽으로 부터 오는 메세지를 필터링하여 output_topic 으로 보내는 예제 입니다.
     * 1. input_topic 과 output_topic 토픽을 생성 합니다.
     * kafka-topics --bootstrap-server 127.0.0.1:9092 --topic input_topic --create --partitions 3 --replication-factor 1
     * kafka-topics --bootstrap-server 127.0.0.1:9092 --topic output_topic --create --partitions 3 --replication-factor 1
     * kafka-topics --bootstrap-server 127.0.0.1:9092 --list
     * 2. output_topic 토픽을 consume 합니다. (600 이상만 데이터가 들어오는지 확인)
     * kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic output_topic --group output --from-beginning
     * 3. kafka-producer-basic 에 있는 SimpleJsonMessageProduce 클래스의 메인 메서드를 실행하여 이벤트를 발송합니다.
     * 4. StreamFilterByEvent 클래스의 메인 메서드를 실행합니다.
     */
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(StreamFilterByEvent.class);

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // topology 생성
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("input_topic");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, v) ->  eventCheck(v) > 800
        );
        filteredStream.to("output_topic");

        //최종적인 토폴로지 생성
        final Topology topology = streamsBuilder.build();

        //만들어진 토폴로지 확인
        logger.info("Topology info = {}",topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, properties);

        // start our streams application
        streams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    public static Integer eventCheck(String jsonString){
        try {
            return jsonParser.parse(jsonString)
                    .getAsJsonObject()
                    .get("qty")
                    .getAsInt();
        }
        catch (NullPointerException e){
            return 0;
        }

    }
}
