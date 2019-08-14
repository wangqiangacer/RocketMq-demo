package com.jacken.mq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;

/**
 * 生产组
 */
public class producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer=new DefaultMQProducer("demo_producer_group");
        //设置nameServer地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //开启DefaultMQProducer
        producer.start();
        //创建消息(String topic, String tags, String keys, byte[] body
        Message message =new Message("Topic_demo","Tags","key_001","王强".getBytes(RemotingHelper.DEFAULT_CHARSET));

        //发送消息
        SendResult result = producer.send(message);
        System.out.println(result);
        producer.shutdown();


    }
}
