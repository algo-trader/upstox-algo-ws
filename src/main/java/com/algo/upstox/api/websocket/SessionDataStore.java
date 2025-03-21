package com.algo.upstox.api.websocket;

import com.algo.upstox.common.service.impl.ApiFactory;
import com.upstox.ApiClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.algo.upstox.api.websocket.MarketDataFeedV3Client.createMarketDataFeedV3Client;
import static com.algo.upstox.api.websocket.PositionFeedClient.createPositionFeedClient;
import static com.algo.upstox.common.config.ApplicationContextProvider.getBean;

public class SessionDataStore {
    private static final Map<String, ApiClient> registeredApiClients = new HashMap<>();
    private static final Map<String, MarketDataFeedV3Client> registeredMarketDataClients = new HashMap<>();
    private static final Map<String, PositionFeedClient> registeredPositionFeedClients = new HashMap<>();

    public static ApiClient getApiClient(String sessionId) {
        var apiFactory = getBean(ApiFactory.class);

        if (!registeredApiClients.containsKey(sessionId)) {
            registeredApiClients.put(sessionId, apiFactory.getApiClient(sessionId));
        }

        return registeredApiClients.get(sessionId);
    }

    public static void registerMarketDataFeedV3Client(String sessionId) {

        if (!registeredMarketDataClients.containsKey(sessionId)) {
            registeredMarketDataClients.put(sessionId, createMarketDataFeedV3Client(sessionId));
        }
    }

    public static void registerPositionFeedClient(String sessionId) {

        if (!registeredPositionFeedClients.containsKey(sessionId)) {
            registeredPositionFeedClients.put(sessionId, createPositionFeedClient(sessionId));
        }
    }

    public static void deRegisterApiClient(String sessionId) {
        registeredApiClients.remove(sessionId);
    }

    public static void deRegisterMarketDataFeedV3Client(String sessionId) {
        registeredMarketDataClients.remove(sessionId).disconnect();
    }

    public static void deRegisterPositionFeedClient(String sessionId) {
        registeredPositionFeedClients.remove(sessionId).disconnect();
    }

    public static void runTaskOnMarketDataFeedV3Client(String sessionId, Consumer<MarketDataFeedV3Client> consumer) {
        Optional.ofNullable(registeredMarketDataClients.get(sessionId))
                .ifPresent(consumer);
    }

    public static void runTaskOnPositionFeedClient(String sessionId, Consumer<PositionFeedClient> consumer) {
        Optional.ofNullable(registeredPositionFeedClients.get(sessionId))
                .ifPresent(consumer);
    }
}
