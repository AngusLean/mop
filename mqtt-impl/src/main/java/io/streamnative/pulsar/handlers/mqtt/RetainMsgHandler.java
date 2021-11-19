package io.streamnative.pulsar.handlers.mqtt;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

public interface RetainMsgHandler {
    /**
     * Add a new rained message to a given topic
     * @param topic of the retained message
     * @param mqttMessage actual message info
     * @return
     */
    CompletableFuture<PositionImpl> put(String topicName, MqttPublishMessage mqttMessage);

    /**
     * remove all message in topic
     * @param topic should be removed
     * @return
     */
    CompletableFuture<Void> remove(String topicName);

    /**
     * get latest retained message
     * @param topicName of the retained message
     * @return message
     */
    List<CompletableFuture<Optional<MqttPublishMessage>>>  get(List<String> topicName, String clientId);

    class MqttRetainMessage extends MqttMessage{

        public MqttRetainMessage(MqttFixedHeader mqttFixedHeader, Object variableHeader, Object payload) {
            super(mqttFixedHeader, variableHeader, payload);
        }
    }
}
