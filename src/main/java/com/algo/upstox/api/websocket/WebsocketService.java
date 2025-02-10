package com.algo.upstox.api.websocket;

import com.algo.upstox.common.service.ConditionalTradeService;
import com.algo.upstox.common.service.OrderService;
import com.algo.upstox.common.service.UserSubscriptionService;
import com.algo.upstox.common.service.impl.ApiFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.ApiException;
import io.swagger.client.api.WebsocketApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.algo.upstox.common.constants.AppConstants.API_VERSION;


@Component
@RequiredArgsConstructor
@Slf4j
public class WebsocketService {
    private final ApiFactory apiFactory;
    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final ConditionalTradeService conditionalTradeService;

    private static final Map<String, AppWebSocketClient> connectedClients = new HashMap<>();

    public void initiateWebsocket(String sessionId) {
        var websocketApi = apiFactory.getApi(sessionId, WebsocketApi.class);
        try {
            var response = websocketApi.getMarketDataFeedAuthorize(API_VERSION);
            var uri = response.getData().getAuthorizedRedirectUri();
            log.info("Portfolio URI - {}", uri);
            var client = createWebSocketClient(uri, sessionId);
            connectedClients.put(sessionId, client);
            client.connect();
        } catch (ApiException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void disconnectWebsocket(String sessionId) {
        Optional.ofNullable(connectedClients.remove(sessionId))
                .ifPresent(WebSocketClient::close);
    }

    private AppWebSocketClient createWebSocketClient(String uri, String sessionId) {
        return new AppWebSocketClient(URI.create(uri), subscriptionService, objectMapper, orderService,
                sessionId, conditionalTradeService);
    }

}
