package com.example;

import com.google.gson.JsonParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

@Service
public class StreamListenerExample {

    @StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
    public String onEventByString(@Payload String message){
        if(eventCheck(message) > 800){
            return 	message;
        }else{
            return null;
        }
    }

//    @Autowired
//    Processor processor;
//
//    @StreamListener(Processor.INPUT)
//    public void onEventByString1(@Payload String message){
//
//        if(eventCheck(message) > 800) {
//            MessageChannel outputChannel = processor.output();
//
//            outputChannel.send(MessageBuilder
//                    .withPayload(message)
//                    .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
//                    .build());
//        }
//    }


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
