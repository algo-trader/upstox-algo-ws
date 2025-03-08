package com.algo.upstox.api.websocket;

import com.algo.upstox.common.exception.AppApiException;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.ConditionalTradeService;
import com.algo.upstox.common.service.OrderService;
import com.algo.upstox.common.service.StraddleTotalPremiumService;
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
    private final StraddleTotalPremiumService straddleTotalPremiumService;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    private static final Map<String, AppWebSocketClient> connectedClients = new HashMap<>();
    private static final Map<String, PositionFeedClient> connectedPositionFeedClients = new HashMap<>();

    public void initiateWebsocket(String sessionId) {
        var webSocketClient = createWebSocketClient(resolveMarketDataFeedURI(sessionId), sessionId);
        var positionFeedClient = createPositionFeedClient(resolvePositionDataFeedURI(sessionId), sessionId);
        connectedClients.put(sessionId, webSocketClient);
        connectedPositionFeedClients.put(sessionId, positionFeedClient);
        webSocketClient.connect();
        positionFeedClient.connect();
    }

    private String resolveMarketDataFeedURI(String sessionId) {
        var websocketApi = apiFactory.getApi(sessionId, WebsocketApi.class);
        try {
            var response = websocketApi.getMarketDataFeedAuthorize(API_VERSION);
            websocketApi.setPositionUpdate(true);
            var uri = response.getData().getAuthorizedRedirectUri();
            log.info("Market data feed URI - {}", uri);
            return uri;
        } catch (ApiException e) {
            log.error(e.getMessage(), e);
            throw new AppApiException(e.getMessage(), e);
        }
    }

    private String resolvePositionDataFeedURI(String sessionId) {
        var websocketApi = apiFactory.getApi(sessionId, WebsocketApi.class);
        try {
            websocketApi.setPositionUpdate(true);
            var portfolioFeedResponse = websocketApi.getPortfolioStreamFeedAuthorize(API_VERSION);
            portfolioFeedResponse.getData().getAuthorizedRedirectUri();
            var uri = portfolioFeedResponse.getData().getAuthorizedRedirectUri();
            log.info("Position Feed URI - {}", uri);
            return uri;
        } catch (ApiException e) {
            log.error(e.getMessage(), e);
            throw new AppApiException(e.getMessage(), e);
        }
    }

    public void loadConditionalTrades(String sessionId) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(AppWebSocketClient::loadConditionalTrade);
    }

    public void deactivateConditionalTrade(String sessionId) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(AppWebSocketClient::deactivateConditionalTrade);
    }

    public void updateExecutingTrades(String sessionId, String tradeId) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(app -> app.updateConditionalTrade(tradeId));
    }

    public void loadStraddleTotalPremiums(String sessionId, IndexEnum index) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(app -> app.fetchStraddleTotalPremium(index));
    }

    public void deleteStraddleTotalPremiums(String sessionId, IndexEnum index) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(app -> app.deleteStraddleTotalPremium(index));
    }

    public void closeRunningTrades(String sessionId) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(AppWebSocketClient::forceCloseTrades);
    }

    public void disconnectWebsocket(String sessionId) {
        Optional.ofNullable(connectedClients.remove(sessionId))
                .ifPresent(WebSocketClient::close);
    }

    public void refreshSubscription(String sessionId) {
        ping(sessionId);
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(AppWebSocketClient::refreshSubscription);
    }

    private AppWebSocketClient createWebSocketClient(String uri, String sessionId) {
        return new AppWebSocketClient(URI.create(uri), subscriptionService, objectMapper, orderService,
                sessionId, websocketMessageEmitter, conditionalTradeService, straddleTotalPremiumService);
    }

    private PositionFeedClient createPositionFeedClient(String uri, String sessionId) {
        return new PositionFeedClient(URI.create(uri), sessionId);
    }

    private void ping(String sessionId) {
        Optional.ofNullable(connectedClients.get(sessionId))
                .ifPresent(client -> {
                    if (!client.isOpen()) {
                        var existingWS = client.getConditionalTradeWs();
                        disconnectWebsocket(sessionId);
                        connectedClients.put(sessionId, new AppWebSocketClient(URI.create(resolveMarketDataFeedURI(sessionId)),
                                subscriptionService, objectMapper, sessionId, websocketMessageEmitter,
                                existingWS, straddleTotalPremiumService));
                    }
                });
    }

}
