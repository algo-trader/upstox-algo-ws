package com.algo.upstox.api.websocket;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;

import static com.algo.upstox.api.websocket.AppWebSocketClient.handleBinaryMessage;

@Slf4j
public class PositionFeedClient extends WebSocketClient {
    private final String sessionId;

    public PositionFeedClient(URI serverUri, String sessionId) {
        super(serverUri);
        this.sessionId = sessionId;
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("::::PositionFeedClient :: onOpen:::: {} - {}", serverHandshake.getHttpStatus(), serverHandshake.getHttpStatusMessage());
    }

    @Override
    public void onMessage(String s) {
        log.info("::::PositionFeedClient :: onMessage:::: {}", s);
    }

    @Override
    public void onMessage(ByteBuffer buffer) {
        log.debug("Received binary message: {}", buffer);
        var response = handleBinaryMessage(buffer);
        log.info("::::PositionFeedClient :: onMessage Bytes :::: {}", response);
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        log.info("::::PositionFeedClient :: onClose:::: {}", s);
    }

    @Override
    public void onError(Exception e) {
        log.error("::::PositionFeedClient :: ERROR::::", e);
    }
}
