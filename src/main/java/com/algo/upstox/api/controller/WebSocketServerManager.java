package com.algo.upstox.api.controller;

import com.algo.upstox.api.WebSocketLoggedInUserService;
import com.algo.upstox.api.model.AlertDisplayTypeEnum;
import com.algo.upstox.api.model.AlertDto;
import com.algo.upstox.api.websocket.WebsocketMessageEmitter;
import com.algo.upstox.api.websocket.WebsocketService;
import com.algo.upstox.common.config.AppPropertyConfig.WebsocketAppConfig;
import com.algo.upstox.common.events.UserLoginEventDto;
import com.algo.upstox.common.model.AlertTypeEnum;
import com.algo.upstox.common.model.OpenInterestResponseDto;
import com.algo.upstox.common.model.WSMessageDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.AuthService;
import com.algo.upstox.common.service.EventService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.algo.upstox.api.util.WSConstants.NATIVE_HEADERS_KEY;
import static com.algo.upstox.api.util.WSConstants.SESSION_ID_KEY;
import static com.algo.upstox.api.util.WSConstants.SIMP_SESSION_ID_KEY;
import static com.algo.upstox.api.util.WSConstants.USER_KEY;
import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterMarketDataFeedV3Client;
import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterPositionFeedClient;
import static com.algo.upstox.api.websocket.SessionDataStore.getMarketDataFeedV3Client;
import static com.algo.upstox.api.websocket.SessionDataStore.getPositionFeedClient;
import static com.algo.upstox.common.constants.MessageConstants.INDEX;
import static com.algo.upstox.common.constants.MessageConstants.TRADE_ID;
import static java.util.Objects.isNull;

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
    private final WebsocketMessageEmitter messageEmitter;
    private final EventService eventService;
    private final ObjectMapper objectMapper;

    @MessageMapping("/user/tradeData")
    public void notificationFromUser(@Header(SESSION_ID_KEY) String sessionId, GenericMessage message) {
        log.info("User pinged for trade data {}", sessionId);
    }

    @MessageMapping("/user/api/activity")
    public void notificationFromApi(GenericMessage<WSMessageDto> message) {
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
            case FORCE_CLOSE -> {
                websocketService.closeRunningTrades(message.getPayload().getSessionId());
            }
            case STRADDLE_CREATE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("Notification received :::: Straddle create {}", index);
                var sessionId = message.getPayload().getSessionId();
                websocketService.loadStraddleTotalPremiums(sessionId, IndexEnum.valueOf(index));
                messageEmitter.emitAlertMessage(AlertDto.builder()
                        .alertText("Straddle created!!")
                        .alertType(AlertTypeEnum.STRADDLE_CREATE)
                        .alertDisplayType(AlertDisplayTypeEnum.SUCCESS.getClassNames())
                        .timeout(5000)
                        .build(), sessionId);

            }
            case STRADDLE_UPDATE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("Notification received :::: Straddle update {}", index);
                var sessionId = message.getPayload().getSessionId();
                websocketService.loadStraddleTotalPremiums(sessionId, IndexEnum.valueOf(index));
                messageEmitter.emitAlertMessage(AlertDto.builder()
                        .alertText("Straddle updated!!")
                        .alertType(AlertTypeEnum.STRADDLE_UPDATE)
                        .alertDisplayType(AlertDisplayTypeEnum.INFO.getClassNames())
                        .timeout(5000)
                        .build(), sessionId);
            }
            case STRADDLE_DELETE -> {
                var index = getNativeHeaderValue(message.getHeaders(), INDEX);
                log.info("Notification received :::: Straddle delete {}", index);
                var sessionId = message.getPayload().getSessionId();
                websocketService.deleteStraddleTotalPremiums(sessionId, IndexEnum.valueOf(index));
                messageEmitter.emitAlertMessage(AlertDto.builder()
                        .alertText("Straddle deleted!!")
                        .alertType(AlertTypeEnum.STRADDLE_DELETE)
                        .alertDisplayType(AlertDisplayTypeEnum.DANGER.getClassNames())
                        .timeout(5000)
                        .build(), sessionId);
            }
            case SUBSCRIBE -> {
                log.debug("Notification received :::: Updated subscription");
                websocketService.refreshSubscription(message.getPayload().getSessionId());
            }
            case EVENT_PUBLISH -> {
                log.debug("Notification received :::: New event published");
                var data = (LinkedHashMap) message.getPayload().getBody();
                var allEvents = eventService.getEvents(authService.getLoggedInUser(message.getPayload().getSessionId()).getEmail());
                messageEmitter.emitEventPublished(allEvents, message.getPayload().getSessionId());

            }
            case ALERT_TRIGGERED -> {
                log.info("Notification received :::: New event published");
                messageEmitter.emitAlertTriggered(message.getPayload().getSessionId());
            }
            case POSITION_FETCHED -> {
                log.info("Notification received :::: User has new position");
                websocketService.fetchPositions(message.getPayload().getSessionId());
            }
            case USER_LOGIN -> {
                log.info("Notification received :::: New user session");
                Optional.ofNullable(message.getPayload())
                        .map(obj -> obj.getBody())
                        .map(obj -> objectMapper.convertValue(obj, UserLoginEventDto.class))
                        .ifPresent(user -> {
                            log.info("::: Clearing old session data for ID {} - Session ID {} ::::", user.getOldSessionId(), user.getSessionId());
                            Optional.ofNullable(getMarketDataFeedV3Client(user.getOldSessionId()))
                                    .ifPresent(client -> client.disconnect());
                            Optional.ofNullable(getPositionFeedClient(user.getOldSessionId()))
                                    .ifPresent(client -> client.disconnect());
                            deRegisterMarketDataFeedV3Client(user.getOldSessionId());
                            deRegisterPositionFeedClient(user.getOldSessionId());
                            websocketService.initiateAllWebsockets(user.getSessionId());
                        });
            }
            case OI_DATA -> {
                log.debug("Notification received :::: Open Interest Data");
                var sessionId = message.getPayload().getSessionId();

                Optional.ofNullable(message.getPayload())
                        .map(obj -> obj.getBody())
                        .map(obj -> objectMapper.convertValue(obj, OpenInterestResponseDto.class))
                        .ifPresent(oiData -> {
                            log.debug("::: Publishing OI Data ::::", oiData);
                            messageEmitter.emitOIData(sessionId, oiData);
                        });
            }
        }
    }

    @EventListener
    public void handleSessionConnected(SessionConnectEvent event) {
        log.info("New client has connected. {}", event.getSource());

        if (checkAppConnectAndInitiate(event)) { // initiates WS
            log.info("API has connected");
            return;
        }

        var simpSessionId = extractSimpSession(event.getMessage());

        var userSessionId = getNativeHeaderValue(event.getMessage().getHeaders(), SESSION_ID_KEY);
        if (isNull(userSessionId)) {
            userSessionId = getNativeHeaderValue(event.getMessage().getHeaders(), USER_KEY);
        }

        log.info("Client details - SimpSession ID - {}, UserSessionId - {}", simpSessionId, userSessionId);

         websocketService.initiateAllWebsockets(userSessionId);
        webSocketLoggedInUserService.updateSessionDetails(userSessionId, simpSessionId);
    }

    @EventListener
    public void handleSessionDisconnected(SessionDisconnectEvent event) {
        log.info("Client is being disconnected. {}", event.getSource());
        var simpSession = extractSimpSession(event.getMessage());

        webSocketLoggedInUserService.retrieveSessionIdBySimpSessionId(simpSession)
                .ifPresent(websocketService::disconnectWebsocket);
    }

    @EventListener
    public void handleSessionSubscribed(SessionSubscribeEvent event) {
        var destination = getNativeHeaderValue(event.getMessage().getHeaders(), "destination");
        var id = getNativeHeaderValue(event.getMessage().getHeaders(), "id");
        log.info("Client Subscribed {} - {}", id, destination);
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
                                log.info(":::: API connected via user {} ::::", user.getUserName());
                                //websocketService.initiateAllWebsockets(user.getSessionId());
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
