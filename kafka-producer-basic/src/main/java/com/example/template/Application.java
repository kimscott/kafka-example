package com.example.template;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Application {

    protected static ApplicationContext applicationContext;
    public static void main(String[] args) {
        applicationContext = SpringApplication.run(Application.class, args);


        // 1. basic example
        ProducerBasic producerBasic = Application.applicationContext.getBean(ProducerBasic.class);
        producerBasic.send_kafka_lib();
        producerBasic.send_kafka_lib_callback();
        producerBasic.send_springboot_lib();
        producerBasic.send_springboot_lib_callback();

        // 2. send with key
//        ProducerKey producerKey = Application.applicationContext.getBean(ProducerKey.class);
//        producerKey.send_kafka_lib();
//        producerKey.send_springboot_lib();

        SpringApplication.exit(applicationContext);

    }
}

