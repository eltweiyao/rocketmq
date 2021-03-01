package org.apache.rocketmq.client;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import static org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
import static org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.RECONSUME_LATER;
import static org.mockito.Mockito.spy;

/**
 * @author cuiweiyao
 * @version DefaultMQProducerSendMsgTest.java, v 0.1 2021-02-10 11:45 cuiweiyao Exp $$
 */

public class DefaultMQClientTest {


    private static DefaultMQProducer producer;
    private static DefaultMQPushConsumer pushConsumer;

    private static String topic = "TopicTest";
    private static String TAG_A = "tagA";

    public static void initProducer() throws MQClientException {
        String producerGroupPrefix = "FooBar_PID";
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setCompressMsgBodyOverHowmuch(16);
        producer.setSendMsgTimeout(10000);
        producer.start();
    }

    //@PostConstruct
    public static void initConsumer() throws MQClientException {
        String consumerGroup = "FooBarGroup1612969956396";
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(60 * 1000);
        pushConsumer.registerMessageListener((MessageListenerOrderly)(msgs, context) -> {
            final MessageExt messageExt = msgs.get(0);
            System.out.println(new String(messageExt.getBody()));
            return ConsumeOrderlyStatus.SUCCESS;
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        PowerMockito.suppress(PowerMockito.method(DefaultMQPushConsumerImpl.class, "updateTopicSubscribeInfoWhenSubscriptionChanged"));

        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();
    }
    public static void sendMsg() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message bigMessage = new Message(topic, TAG_A, ("This is a very huge message!" + System.currentTimeMillis()).getBytes());
        final MessageQueueSelector selector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return mqs.get(11);
            }
        };
        producer.send(bigMessage, 10000);
        //producer.send(bigMessage);

    }

    public static void main(String[] args)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        initConsumer();
        //initProducer();
        //for (int i = 0; i < 3; i++) {
        //    sendMsg();
        //}


    }
}