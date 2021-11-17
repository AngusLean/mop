package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.RetainMsgHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SendRetainedMsgListener implements ChannelFutureListener {
    private final List<String> topicNameList;
    private final String clientId;
    private final RetainMsgHandler retainMsgHandler;
    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {
        List<CompletableFuture<Optional<MqttPublishMessage>>> retainMsgFutures = retainMsgHandler.get(topicNameList, clientId);
        retainMsgFutures.forEach(retainMsgFuture -> retainMsgFuture.thenAccept(mqttRetainMessage -> {
            if(!mqttRetainMessage.isPresent()){
                return;
            }
            channelFuture.channel().write(mqttRetainMessage);
            channelFuture.channel().flush();
            if(log.isDebugEnabled()){
                log.debug("Sending Retain message {} to {}", mqttRetainMessage, NettyUtils.getClientId(channelFuture.channel()));
            }
        }));

    }
}
