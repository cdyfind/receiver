package com.cdy.receiver.controller;

import com.alibaba.fastjson.JSON;
import com.cdy.receiver.constants.RabbitConstants;
import com.cdy.receiver.model.SendMessage;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

/**
 * 发放优惠券的MQ处理
 *
 */
@Controller
@Slf4j
public class SendMessageListener {
    /**
     * 正常监听不消费对队列的信息，只在死信队列中消费队列的消息
     * @param sendMessage
     * @param channel
     * @param message
     * @throws Exception
     */
    //@RabbitListener(queues = RabbitConstants.QUEUE_NAME_SEND_COUPON)
    public void process(String sendMessage, Channel channel, Message message) throws Exception {
        log.info("[{}]处理正常队列消息队列接收数据，消息体：{}", RabbitConstants.QUEUE_NAME_SEND_COUPON, JSON.toJSONString(sendMessage));
        try {
            // 参数校验
            Assert.notNull(sendMessage, "sendMessage 消息体不能为NULL");

            log.info("处理MQ消息");
            // 确认消息已经消费成功
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("MQ消息处理异常，消息ID：{}，消息体:{}", message.getMessageProperties().getCorrelationIdString(),
                    JSON.toJSONString(sendMessage), e);

            // 拒绝当前消息，并把消息返回原队列
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
        }
    }

}
