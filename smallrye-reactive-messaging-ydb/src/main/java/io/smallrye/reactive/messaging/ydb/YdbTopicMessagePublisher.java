package io.smallrye.reactive.messaging.ydb;

import org.eclipse.microprofile.reactive.messaging.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class YdbTopicMessagePublisher extends AbstractReadEventHandler implements Flow.Publisher<Message<tech.ydb.topic.read.Message>> {
    private final AtomicReference<Flow.Subscriber<? super Message<tech.ydb.topic.read.Message>>> exclusiveSubscriber = new AtomicReference<>();

    private final Semaphore subscriberRequests = new Semaphore(0);
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);

    @Override
    public void onMessages(DataReceivedEvent event) {
        List<tech.ydb.topic.read.Message> messages = event.getMessages();
        for (tech.ydb.topic.read.Message message : messages) {
            boolean acquired = false;
            do {
                try {
                    subscriberRequests.acquire();
                    acquired = true;
                } catch (InterruptedException e) {
                    // nothrow
                }
            } while (!acquired);

            exclusiveSubscriber.get().onNext(Message.of(message));
            if (cancelFlag.get() && subscriberRequests.availablePermits() == 0) {
                exclusiveSubscriber.get().onComplete();
            }
        }
        event.commit();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Message<tech.ydb.topic.read.Message>> subscriber) {
        if (!exclusiveSubscriber.compareAndSet(null, subscriber)) {
            subscriber.onError(new IllegalArgumentException());
        }
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                if (!cancelFlag.get()) {
                    subscriberRequests.release((int) n);
                }
            }

            @Override
            public void cancel() {
                cancelFlag.set(true);
            }
        });
    }
}
