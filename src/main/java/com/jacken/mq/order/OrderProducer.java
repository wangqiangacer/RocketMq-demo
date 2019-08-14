package com.jacken.mq.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 顺序生产
 */
public class OrderProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer=new DefaultMQProducer("demo_producer_group");
        //设置nameServer地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //开启DefaultMQProducer
        producer.start();
        //创建消息(String topic, String tags, String keys, byte[] body
        Message message =new Message("Topic_demo","Tags","key_001","王强".getBytes(RemotingHelper.DEFAULT_CHARSET));

        //发送消息，第一个参数是消息体，第二个参数是指定的队列对象（会将所有的消息传进来）第三个参数是指定队列的下标
        for (int i = 0; i < 5; i++) {
            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    //获取队列的下标
                    Integer index=(Integer) o;

                    return list.get(index);
                }
            },0);
            System.out.println(result);
        }

        producer.shutdown();


    }
}
