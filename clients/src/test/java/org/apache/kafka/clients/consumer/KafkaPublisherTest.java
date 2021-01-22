/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Flow;


public class KafkaPublisherTest {

    KafkaConsumer<String, String> consumer;

    @Before
    public void init() {
        HashMap<String, Object> conf = new HashMap<String, Object>();
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.185.8.181:30995");
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Collections.singleton("transcode-message-lp_aliyun"));
    }

    static class TestSubscriber implements Flow.Subscriber<ConsumerRecord<String, String>> {
        Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(ConsumerRecord<String, String> item) {
            System.out.println("item.value() = " + item.value());
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("throwable = " + throwable);
        }

        @Override
        public void onComplete() {
            System.out.println("complete");
        }
    }

    @Test
    public void subscribe() throws InterruptedException {
        consumer.publisher().subscribe(new TestSubscriber());
        synchronized (this) {
            this.wait(20000);
        }
    }
}