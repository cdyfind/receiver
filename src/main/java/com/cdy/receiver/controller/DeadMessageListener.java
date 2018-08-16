package com.cdy.receiver.controller;

import com.alibaba.fastjson.JSON;
import com.cdy.receiver.constants.RabbitConstants;
import com.cdy.receiver.model.SendMessage;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Component
@Slf4j
public class DeadMessageListener {
    /**
     *订单过期判断的话，取出队列的订单号，判断订单状态，如果已支付
     * @param sendMessage
     * @param channel
     * @param message
     * @throws Exception
     */
    @RabbitListener(queues = RabbitConstants.QUEUE_NAME_DEAD_QUEUE)
    public void process(String sendMessage, Channel channel, Message message) throws Exception {
        log.info("[{}]处理死信延迟队列消息队列接收数据，消息体：{}", RabbitConstants.QUEUE_NAME_SEND_COUPON, JSON.toJSONString(sendMessage));

        System.out.println(message.getMessageProperties().getDeliveryTag());

        try {
            // 参数校验
            Assert.notNull(sendMessage, "sendMessage 消息体不能为NULL");

            // TODO 处理消息

            // 确认消息已经消费成功
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("MQ消息处理异常，消息体:{}", message.getMessageProperties().getCorrelationIdString(), JSON.toJSONString(sendMessage), e);

            try {
                // TODO 保存消息到数据库

                // 确认消息已经消费成功
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            } catch (Exception dbe) {
                log.error("保存异常MQ消息到数据库异常，放到死性队列，消息体：{}", JSON.toJSONString(sendMessage), dbe);
                // 确认消息将消息放到死信队列
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
            }
        }
    }
}
