package com.jacken.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class consumer {
    public static void main(String[] args) throws  Exception {
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("demo_consumer_group");
        //指定namesever地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //设置消息最大拉去数
        consumer.setConsumeMessageBatchMaxSize(2);
        //设置subscribe，消费的是哪一个主题
        consumer.subscribe("Topic_demo","Tags");//指定是哪一个主题以及过滤规则(Tags)
        //创建消息监听
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //遍历消息
                for (MessageExt msg : list) {
                    //获取主题
                    String topic = msg.getTopic();
                    String tags = msg.getTags();
                    byte[] body = msg.getBody();
                    try {
                        String s=new String(body, RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("consumer消费信息：topic:"+topic+" tags: "+tags+" result: "+s);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }


                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启condumer
        consumer.start();

    }
}
