package com.cdy.receiver.controller;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RabbitListener(queues = "test1")
public class MQMessageReceiver {
    @RabbitHandler
    public void process(String  context) {
        System.out.println("Receiver : " + context);
    }
}
