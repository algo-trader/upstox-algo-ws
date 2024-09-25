package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.model.TaskListDto;
import com.algo.upstox.service.PositionService;
import com.algo.upstox.service.UserSubscriptionService;
import com.algo.upstox.service.impl.ApiFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.ApiException;
import io.swagger.client.api.WebsocketApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.springframework.stereotype.Component;

import java.net.URI;

import static com.algo.upstox.constants.AppConstants.API_VERSION;

@Component
@RequiredArgsConstructor
@Slf4j
public class WebsocketService {
    private final ApiFactory apiFactory;
    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final FeedResponseEventPublisher feedResponseEventPublisher;
    private final PositionService positionService;

    public void initiateWebsocket(String sessionId, TaskListDto taskList) {
        var websocketApi = apiFactory.getApi(sessionId, WebsocketApi.class);
        try {
            var response = websocketApi.getMarketDataFeedAuthorize(API_VERSION);
            var uri = response.getData().getAuthorizedRedirectUri();
            log.info("Portfolio URI - {}", uri);
            var client = createWebSocketClient(uri, sessionId, taskList);
            client.connect();
        } catch (ApiException e) {
            log.error(e.getMessage(), e);
        }
    }

    private WebSocketClient createWebSocketClient(String uri, String sessionId, TaskListDto taskList) {
        return new AppWebSocketClient(URI.create(uri), subscriptionService, objectMapper,
                feedResponseEventPublisher, positionService, taskList, sessionId);
    }

}
