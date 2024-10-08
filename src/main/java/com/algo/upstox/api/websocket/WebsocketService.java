package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.config.AppPropertyConfig;
import com.algo.upstox.config.AppPropertyConfig.Scrip;
import com.algo.upstox.model.TaskListDto;
import com.algo.upstox.model.platform.IndexEnum;
import com.algo.upstox.service.BreakoutTradeService;
import com.algo.upstox.service.PlaceOrderService;
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
import java.util.Map;

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
    private final PlaceOrderService placeOrderService;
    private final Map<IndexEnum, Scrip> scrips;
    private final BreakoutTradeService breakoutTradeService;
    private final AppPropertyConfig appPropertyConfig;

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
                feedResponseEventPublisher, positionService, placeOrderService,
                taskList, sessionId, scrips, breakoutTradeService, appPropertyConfig);
    }

}
