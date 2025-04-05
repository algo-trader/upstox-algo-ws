package com.algo.upstox.api.websocket;

import com.upstox.feeder.GttUpdate;
import com.upstox.feeder.HoldingUpdate;
import com.upstox.feeder.OrderUpdate;
import com.upstox.feeder.PortfolioDataStreamer;
import com.upstox.feeder.PositionUpdate;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static com.algo.upstox.api.websocket.SessionDataStore.deRegisterPositionFeedClient;
import static com.algo.upstox.api.websocket.SessionDataStore.getApiClient;
import static java.util.Objects.isNull;

@Slf4j
public class PositionFeedClient {
    private final String sessionId;
    private final PortfolioDataStreamer streamer;

    public static PositionFeedClient createPositionFeedClient(String sessionId) {
        var client = SessionDataStore.getPositionFeedClient(sessionId);
        if (isNull(client)) {
            return new PositionFeedClient(sessionId);
        }
        return client;
    }

    private PositionFeedClient(String sessionId) {
        log.info(":::: Creating PositionFeedClient ::::");
        this.sessionId = sessionId;
        this.streamer = initiateStreamer();

        streamer.connect();
    }

    private PortfolioDataStreamer initiateStreamer() {
        var portfolioStreamer = new PortfolioDataStreamer(getApiClient(sessionId), true, true, true, true);

        portfolioStreamer.setOnHoldingUpdateListener(this::onHoldingUpdate);
        portfolioStreamer.setOnOrderUpdateListener(this::onOrderUpdate);
        portfolioStreamer.setOnPositionUpdateListener(this::onPositionUpdate);
        portfolioStreamer.setOnGttUpdateListener(this::onGttUpdate);

        portfolioStreamer.setOnErrorListener(this::onError);
        portfolioStreamer.setOnCloseListener(this::onClose);
        portfolioStreamer.setOnOpenListener(this::onOpen);

        portfolioStreamer.autoReconnect(true, 3, 15);

        return portfolioStreamer;
    }

    private void onGttUpdate(GttUpdate gttUpdate) {
        log.info("GTTUpdate received: {}", gttUpdate);
    }

    private void onPositionUpdate(PositionUpdate positionUpdate) {
        log.info("onPositionUpdate: {}", positionUpdate);
    }

    private void onOrderUpdate(OrderUpdate orderUpdate) {
        log.info("::: PositionFeedClient onOrderUpdate ::: {}", orderUpdate);
        Optional.ofNullable(SessionDataStore.getMarketDataFeedV3Client(sessionId))
                .ifPresent(client -> {
                    client.fetchPositions();
                    client.fetchHoldings();
                });
    }

    private void onHoldingUpdate(HoldingUpdate holdingUpdate) {
        log.info("onHoldingUpdate: {}", holdingUpdate);
    }

    public void onOpen() {
        log.info("::::PositionFeedClient :: onOpen::::");
    }

    public void onClose(int i, String s) {
        log.info("::::PositionFeedClient :: onClose:::: {}", s);
        deRegisterPositionFeedClient(sessionId);
    }

    public void onError(Throwable e) {
        log.error("::::PositionFeedClient :: ERROR::::", e);
        deRegisterPositionFeedClient(sessionId);
    }

    public void disconnect() {
        log.info("::::PositionFeedClient :: disconnect");
        this.streamer.disconnect();
    }
}
