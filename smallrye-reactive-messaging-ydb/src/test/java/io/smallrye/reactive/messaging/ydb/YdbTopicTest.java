package io.smallrye.reactive.messaging.ydb;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Flow;

public class YdbTopicTest extends YdbTopicTestBase {
    private static final String TOPIC_TAME = "topic-name";

    @Test
    public void test() {
        createTopic(TOPIC_TAME);

        sendMessages(TOPIC_TAME, "test-1".getBytes(), "test-2".getBytes());

        YdbTopicConnector ydbTopicConnector = new YdbTopicConnector();
        Flow.Publisher<? extends Message<?>> publisher = ydbTopicConnector.getPublisher(initConfiguration(TOPIC_TAME));

        AssertSubscriber<Object> objectAssertSubscriber = AssertSubscriber.create(2);
        publisher.subscribe(objectAssertSubscriber);

        objectAssertSubscriber.awaitSubscription();
        objectAssertSubscriber.assertSubscribed();
        objectAssertSubscriber.awaitNextItems(2, Duration.ofDays(1));
        objectAssertSubscriber.cancel();

        deleteTopic(TOPIC_TAME);
    }
}
