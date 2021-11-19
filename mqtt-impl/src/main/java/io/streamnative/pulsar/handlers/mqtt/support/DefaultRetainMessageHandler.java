package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.RetainMsgHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

public class DefaultRetainMessageHandler implements RetainMsgHandler {
    private final PulsarService pulsarService;
    private final MQTTServerConfiguration configuration;

    public DefaultRetainMessageHandler(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
        init(pulsarService, configuration);
    }

    private void init(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        try {
            String retainTopicNameSpace = NamespaceName.get(configuration.getDefaultTenant(),
                    configuration.getDefaultRetainNamespace()).toString();
            PulsarAdmin adminClient = pulsarService.getAdminClient();

            Set<String> namespaces =
                    new HashSet<String>(adminClient.namespaces().getNamespaces(configuration.getDefaultTenant()));
            if (namespaces.isEmpty() || !namespaces.contains(retainTopicNameSpace)) {
                adminClient.namespaces().createNamespace(retainTopicNameSpace);
                adminClient.namespaces().setRetention(retainTopicNameSpace,
                        new RetentionPolicies(-1, -1));
            }

        } catch (PulsarServerException | PulsarAdminException e) {
            throw new MQTTServerException(e);
        }
    }

    @Override
    public CompletableFuture<PositionImpl> put(String topicName, MqttPublishMessage mqttMessage) {
        CompletableFuture<Optional<Topic>> retainTopicFuture = createAndGetTopicRef(topicName);
        return retainTopicFuture.thenCompose(tp -> {
            if (!tp.isPresent()) {
                createNewRetainTopic(topicName);
            }
            Topic retainTopic = tp.get();
            MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(retainTopic, mqttMessage);
            CompletableFuture<PositionImpl> ret = MessagePublishContext.publishMessages(message, retainTopic);
            return ret;
        });
    }

    private void createNewRetainTopic(String topicName) {
        try {
            pulsarService.getAdminClient().topics().createPartitionedTopic(topicName, 3);
        } catch (PulsarAdminException | PulsarServerException e) {
            throw new MQTTServerException(e);
        }
    }

    @Override
    public CompletableFuture<Void> remove(String topicName) {
        CompletableFuture<Optional<Topic>> retainTopicFuture = getRetainTopic(topicName);
        return retainTopicFuture.thenAccept(tpOp -> {
            if (!tpOp.isPresent()) {
                createNewRetainTopic(topicName);
                return;
            }
            try {
                PulsarAdmin adminClient = pulsarService.getAdminClient();
                adminClient.topics().expireMessagesForAllSubscriptions(topicName, 0);
            } catch (PulsarServerException | PulsarAdminException e) {
                throw new MQTTServerException(e);
            }
        });
    }

    @Override
    public List<CompletableFuture<Optional<MqttPublishMessage>>> get(List<String> topicNameList, String clientId) {
        List<CompletableFuture<Optional<MqttPublishMessage>>> result = new ArrayList<>();
        for (String topicName : topicNameList) {
            CompletableFuture<Optional<Topic>> retainTopicFuture = getRetainTopic(topicName);
            result.add(retainTopicFuture.thenApply(tpOp -> {
                if (!tpOp.isPresent()) {
                    throw new MQTTServerException("retain topic not present");
                }
                String newTopicName = tpOp.get().getName();
                try {
                    Reader reader = pulsarService.getClient().newReader()
                            .topic(newTopicName)
                            .startMessageId(MessageId.earliest)
                            .create();
                    if (!reader.hasMessageAvailable()) {
                        return Optional.empty();
                    }
                    Message<byte[]> message = reader.readNext();
                    return Optional.of(MqttMessageUtils.createPublishMessage(newTopicName, message.getData(),
                            MqttQoS.AT_LEAST_ONCE, true));
                } catch (PulsarServerException | PulsarClientException e) {
                    throw new MQTTServerException(e);
                }
            }));
        }
        return result;
    }


    private CompletableFuture<Optional<Topic>> getRetainTopic(String topicName) {
        return PulsarTopicUtils.getTopicReference(pulsarService, topicName,
                configuration.getDefaultTenant(), configuration.getDefaultRetainNamespace(), false,
                configuration.getDefaultTopicDomain());
    }

    private CompletableFuture<Optional<Topic>> createAndGetTopicRef(String topicName) {
        String pulsarTopicName = PulsarTopicUtils.getPulsarTopicName(topicName, configuration.getDefaultTenant(),
                configuration.getDefaultRetainNamespace(), false,
                TopicDomain.persistent);

        try {
            pulsarService.getAdminClient().topics().createPartitionedTopic(pulsarTopicName, 4);
        } catch (PulsarAdminException | PulsarServerException e) {
            throw new MQTTServerException(e);
        }
        return PulsarTopicUtils.getTopicReference(pulsarService, topicName,
                configuration.getDefaultTenant(), configuration.getDefaultRetainNamespace(), false,
                configuration.getDefaultTopicDomain());
    }

}
