package com.example;

import com.google.gson.JsonParser;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

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
