package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
                consume(left);
        }

        private void consume(int n) {
            final Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords = kafkaPublisher.fetcher.fetchedRecords(n);
            if (!fetchedRecords.isEmpty()) {
                ConsumerRecords<K, V> consumerRecords = new ConsumerRecords<>(fetchedRecords);
                left = n - consumerRecords.count();
                kafkaPublisher.executorService.execute(() -> {
                    consumerRecords.forEach(kvConsumerRecord -> {
                        subscriber.onNext(kvConsumerRecord);
                    });
                    if (closed) subscriber.onComplete();
                });
            } else {
                left = n;
            }
        }

        @Override
        public void request(long n) {
            kafkaPublisher.fetcher.sendFetches();
            consume((int) n);
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
