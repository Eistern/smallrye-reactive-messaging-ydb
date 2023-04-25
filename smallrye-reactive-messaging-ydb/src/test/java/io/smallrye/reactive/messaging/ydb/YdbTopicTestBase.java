package io.smallrye.reactive.messaging.ydb;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.InitResult;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class YdbTopicTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(YdbTopicTestBase.class);

    protected static GenericContainer<?> YDB_CONTAINER;

    protected static TopicClient YDB_ADMIN_CLIENT;

    @BeforeAll
    public static void startYdbContainer() {
        YDB_CONTAINER = new FixedHostPortGenericContainer<>("cr.yandex/yc/yandex-docker-local-ydb:latest")
                .withFixedExposedPort(2135, 2135)
                .withFixedExposedPort(2136, 2136)
                .withFixedExposedPort(8765, 8765)
                .withEnv(Map.of(
                        "GRPC_TLS_PORT", "2135", "GRPC_PORT", "2136", "MON_PORT", "8765"
                ))
                .withReuse(true)
                .withAccessToHost(true)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("localhost"))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER)
                        .withSeparateOutputStreams())
                .waitingFor(new DockerHealthcheckWaitStrategy());
        YDB_CONTAINER.start();
        Integer mappedPort = YDB_CONTAINER.getMappedPort(2136);
        YDB_ADMIN_CLIENT = TopicClient.newClient(
                GrpcTransport
                        .forHost("localhost", mappedPort, "/local")
                        .build())
                .build();
    }

    @AfterAll
    public static void stopYdbContainer() {
        if (YDB_CONTAINER != null) {
            YDB_CONTAINER.stop();
        }
    }

    public MapBasedConfig initConfiguration(String topic) {
        Map<String, Object> configMap = Map.of(
                "endpoint-uri", "grpc://localhost:2136",
                "ydb-name", "/local",
                "topic-name", topic,
                "topic-consumer", "test-consumer");

        return new MapBasedConfig(configMap);
    }

    public static void sendMessages(String topic, byte[]... data) {
        SyncWriter syncWriter = YDB_ADMIN_CLIENT.createSyncWriter(WriterSettings.newBuilder()
                .setCodec(Codec.RAW)
                .setTopicPath(topic)
                .setProducerId("test-producer")
                .setMessageGroupId("test-producer")
                .build());
        InitResult initResult = syncWriter.initAndWait();

        System.out.println(initResult);

        for (byte[] message : data) {
            syncWriter.send(Message.of(message));
        }
        syncWriter.flush();
        try {
            syncWriter.shutdown(1, TimeUnit.DAYS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTopic(String topic) {
        CompletableFuture<Status> topicFuture = YDB_ADMIN_CLIENT.createTopic(topic, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder()
                        .setName("test-consumer")
                        .setSupportedCodecs(SupportedCodecs.newBuilder()
                                .addCodec(Codec.RAW)
                                .build())
                        .build())
                .setSupportedCodecs(SupportedCodecs.newBuilder()
                        .addCodec(Codec.RAW)
                        .build())
                .build());
        topicFuture.join();
    }

    public static void deleteTopic(String topic) {
        YDB_ADMIN_CLIENT.dropTopic(topic).join();
    }
}
