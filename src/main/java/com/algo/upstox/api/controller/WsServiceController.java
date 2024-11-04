package com.algo.upstox.api.controller;

import com.algo.upstox.api.websocket.WebsocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Controller
@RequiredArgsConstructor
@Slf4j
public class WsServiceController {
    private final SimpMessagingTemplate messagingTemplate;
    private final WebsocketService websocketService;

    @MessageMapping("/user")
    public void sendMessage(SimpMessageHeaderAccessor headerAccessor, @DestinationVariable String sessionId) {

    }

    @EventListener
    public void handleSessionConnected(SessionConnectEvent event) {
        log.info("New client has connected. {}", event.getSource());
    }

    @EventListener
    public void handleSessionConnected(SessionDisconnectEvent event) {
        log.info("Client has disconnected. {}", event.getSource());
    }


}
