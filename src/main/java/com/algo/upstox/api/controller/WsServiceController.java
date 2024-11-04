package com.algo.upstox.api.controller;

import com.algo.upstox.api.websocket.WebsocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

@Controller
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings("all")
public class WsServiceController {
    private final SimpMessagingTemplate messagingTemplate;
    private final WebsocketService websocketService;

    @MessageMapping("/user")
    public void sendMessage(SimpMessageHeaderAccessor headerAccessor, @DestinationVariable String sessionId) {

    }

    @EventListener
    public void handleSessionConnected(SessionConnectEvent event) {
        log.info("New client has connected. {}", event.getSource());
        var simpSessionId = Optional.of(event.getMessage())
                .map(Message::getHeaders)
                .stream().filter(h -> h.containsKey("simpSessionId"))
                .findFirst()
                .map(h -> (String) h.get("simpSessionId"))
                .orElse(null);

        var userSessionId = Optional.of(event.getMessage())
                .map(Message::getHeaders)
                .stream()
                .filter(h -> h.containsKey("nativeHeaders"))
                .findFirst()
                .map(h -> (Map) h.get("nativeHeaders"))
                .map(map -> map.get("sessionId"))
                .map(sessionList -> (ArrayList) sessionList)
                .map(list -> (String) list.get(0))
                .orElse(null);

        log.info("Client details - SimpSession ID - {}, UserSessionId - {}", simpSessionId, userSessionId);
    }

    @EventListener
    public void handleSessionConnected(SessionDisconnectEvent event) {
        log.info("Client has disconnected. {}", event.getSource());
    }


}
