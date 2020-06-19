package com.example.template;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Application {

    protected static ApplicationContext applicationContext;
    public static void main(String[] args) {
        applicationContext = SpringApplication.run(Application.class, args);

        // 1. 컨슈머
//        ConsumerBasic consumerBasic = Application.applicationContext.getBean(ConsumerBasic.class);
//        consumerBasic.listen_kafka_lib();

    }
}

