/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageListener;
import io.openmessaging.OMS;
import io.openmessaging.PropertyKeys;
import io.openmessaging.PushConsumer;
import io.openmessaging.ReceivedMessageContext;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.rocketmq.OMSUtil;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class PushConsumerImpl implements PushConsumer {
    private final DefaultMQPushConsumer rocketmqPushConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final Map<String, MessageListener> subscribeTable = new ConcurrentHashMap<>();


    public PushConsumerImpl(final KeyValue properties) {
        this.rocketmqPushConsumer = new DefaultMQPushConsumer();
        this.properties = properties;

        String accessPoints = properties.getString(PropertyKeys.ACCESS_POINTS);
        if (accessPoints == null || accessPoints.isEmpty()) {
            throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
        }
        this.rocketmqPushConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));

        String consumerGroup = properties.getString(NonStandardKeys.CONSUMER_GROUP);
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException("-1", "Consumer Group is necessary for RocketMQ, please set it.");
        }
        this.rocketmqPushConsumer.setConsumerGroup(consumerGroup);

        int maxReDeliveryTimes = properties.getInt(NonStandardKeys.MAX_REDELIVERY_TIMES);
        if (maxReDeliveryTimes != 0) {
            this.rocketmqPushConsumer.setMaxReconsumeTimes(maxReDeliveryTimes);
        }

        int messageConsumeTimeout = properties.getInt(NonStandardKeys.MESSAGE_CONSUME_TIMEOUT);
        if (messageConsumeTimeout != 0) {
            this.rocketmqPushConsumer.setConsumeTimeout(messageConsumeTimeout);
        }

        int maxConsumeThreadNums = properties.getInt(NonStandardKeys.MAX_CONSUME_THREAD_NUMS);
        if (maxConsumeThreadNums != 0) {
            this.rocketmqPushConsumer.setConsumeThreadMax(maxConsumeThreadNums);
        }

        int minConsumeThreadNums = properties.getInt(NonStandardKeys.MIN_CONSUME_THREAD_NUMS);
        if (minConsumeThreadNums != 0) {
            this.rocketmqPushConsumer.setConsumeThreadMin(minConsumeThreadNums);
        }

        String consumerId = OMSUtil.buildInstanceName();
        this.rocketmqPushConsumer.setInstanceName(consumerId);
        properties.put(PropertyKeys.CONSUMER_ID, consumerId);

        this.rocketmqPushConsumer.registerMessageListener(new MessageListenerImpl());
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void resume() {
        this.rocketmqPushConsumer.resume();
    }

    @Override
    public void suspend() {
        this.rocketmqPushConsumer.suspend();
    }

    @Override
    public boolean isSuspended() {
        return this.rocketmqPushConsumer.getDefaultMQPushConsumerImpl().isPause();
    }

    @Override
    public PushConsumer attachQueue(final String queueName, final MessageListener listener) {
        this.subscribeTable.put(queueName, listener);
        try {
            this.rocketmqPushConsumer.subscribe(queueName, "*");
        } catch (MQClientException e) {
            throw new OMSRuntimeException("-1", String.format("RocketMQ push consumer can't attach to %s.", queueName));
        }
        return this;
    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.rocketmqPushConsumer.start();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.rocketmqPushConsumer.shutdown();
        }
        this.started = false;
    }

    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList, ConsumeConcurrentlyContext contextRMQ) {
            MessageExt rmqMsg = rmqMsgList.get(0);
            BytesMessage omsMsg = OMSUtil.msgConvert(rmqMsg);

            MessageListener listener = PushConsumerImpl.this.subscribeTable.get(rmqMsg.getTopic());

            if (listener == null) {
                throw new OMSRuntimeException("-1",
                    String.format("The topic/queue %s isn't attached to this consumer", rmqMsg.getTopic()));
            }

            final KeyValue contextProperties = OMS.newKeyValue();
            final CountDownLatch sync = new CountDownLatch(1);

            ReceivedMessageContext context = new ReceivedMessageContext() {
                @Override
                public KeyValue properties() {
                    return contextProperties;
                }

                @Override
                public void ack() {
                    sync.countDown();
                    contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS,
                        ConsumeConcurrentlyStatus.CONSUME_SUCCESS.name());
                }

                @Override
                public void ack(final KeyValue properties) {
                    sync.countDown();
                    contextProperties.put(NonStandardKeys.MESSAGE_CONSUME_STATUS,
                        properties.getString(NonStandardKeys.MESSAGE_CONSUME_STATUS));
                }
            };
            listener.onMessage(omsMsg, context);
            try {
                sync.await(PushConsumerImpl.this.rocketmqPushConsumer.getConsumeTimeout(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignore) {
            }

            return ConsumeConcurrentlyStatus.valueOf(contextProperties.getString(NonStandardKeys.MESSAGE_CONSUME_STATUS));
        }
    }
}