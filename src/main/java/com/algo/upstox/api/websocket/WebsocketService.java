package com.algo.upstox.api.websocket;

import com.algo.upstox.common.model.platform.IndexEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterApiClient;
import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterMarketDataFeedV3Client;
import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterPositionFeedClient;
import static com.algo.upstox.api.websocket.SessionDataStore.registerMarketDataFeedV3Client;
import static com.algo.upstox.api.websocket.SessionDataStore.registerPositionFeedClient;
import static com.algo.upstox.api.websocket.SessionDataStore.runTaskOnMarketDataFeedV3Client;


@Component
@RequiredArgsConstructor
@Slf4j
public class WebsocketService {

    public void initiateAllWebsockets(String sessionId) {
       registerMarketDataFeedV3Client(sessionId);
       registerPositionFeedClient(sessionId);
    }

    public void loadConditionalTrades(String sessionId) {
        runTaskOnMarketDataFeedV3Client(sessionId, MarketDataFeedV3Client::loadConditionalTrade);
    }

    public void deactivateConditionalTrade(String sessionId) {
        runTaskOnMarketDataFeedV3Client(sessionId, MarketDataFeedV3Client::deactivateConditionalTrade);
    }

    public void updateExecutingTrades(String sessionId, String tradeId) {
        runTaskOnMarketDataFeedV3Client(sessionId, app -> app.updateConditionalTrade(tradeId));
    }

    public void loadStraddleTotalPremiums(String sessionId, IndexEnum index) {
        runTaskOnMarketDataFeedV3Client(sessionId, app -> app.fetchStraddleTotalPremium(index));
    }

    public void deleteStraddleTotalPremiums(String sessionId, IndexEnum index) {
        runTaskOnMarketDataFeedV3Client(sessionId, app -> app.deleteStraddleTotalPremium(index));
    }

    public void closeRunningTrades(String sessionId) {
        runTaskOnMarketDataFeedV3Client(sessionId, MarketDataFeedV3Client::forceCloseTrades);
    }

    public void disconnectWebsocket(String sessionId) {
        deRegisterApiClient(sessionId);
        deRegisterPositionFeedClient(sessionId);
        deRegisterMarketDataFeedV3Client(sessionId);
    }

    public void refreshSubscription(String sessionId) {
        runTaskOnMarketDataFeedV3Client(sessionId, MarketDataFeedV3Client::refreshSubscription);
    }

}
