package com.algo.upstox.api.controller;

import com.algo.upstox.api.WebSocketLoggedInUserService;
import com.algo.upstox.api.websocket.WebsocketService;
import com.algo.upstox.common.config.AppPropertyConfig.WebsocketAppConfig;
import com.algo.upstox.common.model.WSMessageDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.model.ws.ActivityEnum;
import com.algo.upstox.common.service.AuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.algo.upstox.api.util.WSConstants.NATIVE_HEADERS_KEY;
import static com.algo.upstox.api.util.WSConstants.SESSION_ID_KEY;
import static com.algo.upstox.api.util.WSConstants.SIMP_SESSION_ID_KEY;
import static com.algo.upstox.common.constants.MessageConstants.INDEX;
import static com.algo.upstox.common.constants.MessageConstants.TRADE_ID;

@Controller
@RequiredArgsConstructor
@Slf4j
@SuppressWarnings("all")
public class WebSocketServerManager {
    private final SimpMessagingTemplate messagingTemplate;
    private final WebsocketService websocketService;
    private final WebSocketLoggedInUserService webSocketLoggedInUserService;
    private final AuthService authService;
    private final WebsocketAppConfig websocketAppConfig;

    @MessageMapping("/user/tradeData")
    public void notificationFromUser(@Header(SESSION_ID_KEY) String sessionId, GenericMessage message) {
        log.info("User pinged for trade data {}", sessionId);
    }

    @MessageMapping("/user/api/activity")
    public void notificationFromApi(GenericMessage<WSMessageDto> message) {
        log.info("User pinged for trade data {} - {}", message.getHeaders(), message.getPayload().getActivity());
        var activity = message.getPayload().getActivity();

        switch (activity) {
            case CONDITIONAL_UPDATE -> {
                var tradeId = getNativeHeaderValue(message.getHeaders(), TRADE_ID);
                websocketService.updateExecutingTrades(message.getPayload().getSessionId(), tradeId);
            }
            case CONDITIONAL_CREATE -> {
                websocketService.loadConditionalTrades(message.getPayload().getSessionId());
            }
            case CONDITIONAL_DELETE -> {
                websocketService.deactivateConditionalTrade(message.getPayload().getSessionId());
            }
            case STRADDLE_CREATE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("User pinged for straddle create {}", index);
                websocketService.loadStraddleTotalPremiums(message.getPayload().getSessionId(), IndexEnum.valueOf(index));
            }
            case STRADDLE_UPDATE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("User pinged for straddle update {}", index);
                websocketService.loadStraddleTotalPremiums(message.getPayload().getSessionId(), IndexEnum.valueOf(index));
            }
            case STRADDLE_DELETE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("User pinged for straddle delete {}", index);
                websocketService.deleteStraddleTotalPremiums(message.getPayload().getSessionId(), IndexEnum.valueOf(index));
            }
        }
    }

    @EventListener
    public void handleSessionConnected(SessionConnectEvent event) {
        log.info("New client has connected. {}", event.getSource());

        if (checkAppConnectAndInitiate(event)) {
            log.info("API has connected");
            return;
        }

        var simpSessionId = extractSimpSession(event.getMessage());

        var userSessionId = getNativeHeaderValue(event.getMessage().getHeaders(), SESSION_ID_KEY);

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

    private boolean checkAppConnectAndInitiate(SessionConnectEvent event) {
        return Optional.ofNullable(event.getMessage())
                .map(Message::getHeaders)
                .filter(h -> h.containsKey(NATIVE_HEADERS_KEY))
                .map(h -> (Map) h.get(NATIVE_HEADERS_KEY))
                .map(
                        h -> {
                            var containsConnectionHeader = h.containsKey(websocketAppConfig.getHeaderConnectKey())
                                    && ((List) h.get(websocketAppConfig.getHeaderConnectKey()))
                                    .contains(websocketAppConfig.getHeaderConnectValue());

                            var containsSessionId = h.containsKey(websocketAppConfig.getHeaderUserKey())
                                    && !((List) h.get(websocketAppConfig.getHeaderUserKey())).isEmpty();

                            if (!containsConnectionHeader || !containsSessionId) {
                                return false;
                            }
                            try {
                                String sessionId = (String) ((List<?>) h.get(websocketAppConfig.getHeaderUserKey())).get(0);
                                var user = authService.getLoggedInUser(sessionId);
                                log.info("API connected via user {}", user.getUserName());
                                websocketService.initiateWebsocket(user.getSessionId());
                                return true;
                            } catch (Exception e) {
                                log.error("Unable to authenticate user", e);
                                return false;
                            }
                        })
                .orElse(false);
    }

    private String getNativeHeaderValue(MessageHeaders headers, String key) {
        return Optional.ofNullable(headers)
                .stream()
                .filter(h -> h.containsKey(NATIVE_HEADERS_KEY))
                .findFirst()
                .map(h -> (Map) h.get(NATIVE_HEADERS_KEY))
                .map(map -> map.get(key))
                .map(list -> (ArrayList) list)
                .map(list -> (String) list.get(0))
                .orElse(null);
    }

}
