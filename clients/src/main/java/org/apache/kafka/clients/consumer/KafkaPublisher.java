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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;

public class KafkaPublisher<K, V> implements Flow.Publisher<ConsumerRecord<K, V>> {

    final Fetcher<K, V> fetcher;

    ExecutorService executorService = ForkJoinPool.commonPool();

    public KafkaPublisher(Fetcher<K, V> fetcher) {
        this.fetcher = fetcher;
    }

    static class KafkaSubscription<K, V> implements Flow.Subscription {
        KafkaPublisher<K, V> kafkaPublisher;
        Flow.Subscriber<? super ConsumerRecord<K, V>> subscriber;
        volatile boolean closed = false;
        int left = 0;

        public KafkaSubscription(KafkaPublisher<K, V> kafkaPublisher, Flow.Subscriber<? super ConsumerRecord<K, V>> subscriber) {
            this.kafkaPublisher = kafkaPublisher;
            this.subscriber = subscriber;
            kafkaPublisher.fetcher.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ClientResponse value) {
                    wakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {

                }
            });
        }

        public void wakeup() {
            if (left > 0)
                consume();
        }

        private void consume() {
            final Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords = kafkaPublisher.fetcher.fetchedRecords(left);
            if (!fetchedRecords.isEmpty()) {
                ConsumerRecords<K, V> consumerRecords = new ConsumerRecords<>(fetchedRecords);
                left = left - consumerRecords.count();
                kafkaPublisher.executorService.execute(() -> {
                    consumerRecords.forEach(kvConsumerRecord -> {
                        subscriber.onNext(kvConsumerRecord);
                    });
                    if (closed) subscriber.onComplete();
                });
            }
        }

        @Override
        public void request(long n) {
            left = (int) n;
            kafkaPublisher.fetcher.sendAndTransmitFetches();
            while (left > 0) {
                consume();
                kafkaPublisher.fetcher.pollNow();
                Thread.onSpinWait();
            }
        }

        @Override
        public void cancel() {
            kafkaPublisher.fetcher.close();
        }
    }


    @Override
    public void subscribe(Flow.Subscriber<? super ConsumerRecord<K, V>> subscriber) {
        KafkaSubscription<K, V> subscription = new KafkaSubscription<>(this, subscriber);
        subscriber.onSubscribe(subscription);
    }
}
