package com.algo.upstox.api.controller;

import com.algo.upstox.api.WebSocketLoggedInUserService;
import com.algo.upstox.api.websocket.WebsocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static com.algo.upstox.api.util.WSConstants.NATIVE_HEADERS_KEY;
import static com.algo.upstox.api.util.WSConstants.SESSION_ID_KEY;
import static com.algo.upstox.api.util.WSConstants.SIMP_SESSION_ID_KEY;

@Controller
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings("all")
public class WebSocketServerManager {
    private final SimpMessagingTemplate messagingTemplate;
    private final WebsocketService websocketService;
    private final WebSocketLoggedInUserService webSocketLoggedInUserService;

    @MessageMapping("/user/tradeData")
    public void notificationFromUser(@Header(SESSION_ID_KEY) String sessionId, GenericMessage message) {
        log.info("User pinged for trade data {}", sessionId);
    }

    @EventListener
    public void handleSessionConnected(SessionConnectEvent event) {
        log.info("New client has connected. {}", event.getSource());
        var simpSessionId = extractSimpSession(event.getMessage());

        var userSessionId = Optional.of(event.getMessage())
                .map(Message::getHeaders)
                .stream()
                .filter(h -> h.containsKey(NATIVE_HEADERS_KEY))
                .findFirst()
                .map(h -> (Map) h.get(NATIVE_HEADERS_KEY))
                .map(map -> map.get(SESSION_ID_KEY))
                .map(sessionList -> (ArrayList) sessionList)
                .map(list -> (String) list.get(0))
                .orElse(null);

        log.info("Client details - SimpSession ID - {}, UserSessionId - {}", simpSessionId, userSessionId);

        websocketService.initiateWebsocket(userSessionId);

        webSocketLoggedInUserService.updateSessionDetails(userSessionId, simpSessionId);
    }

    @EventListener
    public void handleSessionConnected(SessionDisconnectEvent event) {
        log.info("Client is being disconnected. {}", event.getSource());
        var simpSession = extractSimpSession(event.getMessage());

        webSocketLoggedInUserService.retrieveSessionIdBySimpSessionId(simpSession)
                .ifPresent(websocketService::disconnectWebsocket);
    }

    @EventListener
    public void handleSessionSubscribed(SessionSubscribeEvent event) {
        log.info("Client Subscribed {}", event.getSource());
    }

    private String extractSimpSession(Message<byte[]> message) {
        return Optional.of(message)
                .map(Message::getHeaders)
                .stream().filter(h -> h.containsKey(SIMP_SESSION_ID_KEY))
                .findFirst()
                .map(h -> (String) h.get(SIMP_SESSION_ID_KEY))
                .orElse(null);
    }

}
