package io.smallrye.reactive.messaging.ydb;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

// Grpc Transport Attributes
@ConnectorAttribute(name = "endpoint-uri", description = "The URI of the YDB endpoint (read from or written to)", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "ydb-name", description = "The YDB name for the connection", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)

@ConnectorAttribute(name = "compression-pool", description = "Number of threads allocated for the compression pool", type = "Integer", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "topic-name", description = "YDB name of the topic", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "topic-consumer", description = "YDB name of the consumer", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING)

@ApplicationScoped
@Connector(YdbTopicConnector.CONNECTOR_NAME)
public class YdbTopicConnector implements InboundConnector {
    public static final String CONNECTOR_NAME = "smallrye-ydb";

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        YdbTopicConnectorIncomingConfiguration configuration = new YdbTopicConnectorIncomingConfiguration(config);

        GrpcTransport topicTransport = GrpcTransport.forEndpoint(
                configuration.getEndpointUri(),
                configuration.getYdbName()).build();

        TopicClient topicClient = TopicClient.newClient(topicTransport)
                .setCompressionPoolThreadCount(configuration.getCompressionPool()
                        .orElse(null))
                .build();

        YdbTopicMessagePublisher publisher = new YdbTopicMessagePublisher();

        AsyncReader asyncReader = topicClient.createAsyncReader(
                ReaderSettings.newBuilder()
                        .setReaderName(CONNECTOR_NAME)
                        .addTopic(TopicReadSettings.newBuilder()
                                .setPath(configuration.getTopicName())
                                .build())
                        .setConsumerName(configuration.getTopicConsumer())
                        .build(),
                ReadEventHandlersSettings.newBuilder()
                        .setEventHandler(publisher)
                        .build());

        asyncReader.init().join();

        return publisher;
    }
}
