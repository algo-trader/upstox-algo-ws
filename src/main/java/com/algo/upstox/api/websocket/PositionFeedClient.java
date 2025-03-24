package com.algo.upstox.api.websocket;

import com.upstox.feeder.GttUpdate;
import com.upstox.feeder.HoldingUpdate;
import com.upstox.feeder.OrderUpdate;
import com.upstox.feeder.PortfolioDataStreamer;
import com.upstox.feeder.PositionUpdate;
import lombok.extern.slf4j.Slf4j;

import static com.algo.upstox.api.websocket.SessionDataStore.getApiClient;

@Slf4j
public class PositionFeedClient {
    private final String sessionId;
    private final PortfolioDataStreamer streamer;

    public static PositionFeedClient createPositionFeedClient(String sessionId) {
        return new PositionFeedClient(sessionId);
    }

    private PositionFeedClient(String sessionId) {
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
        log.info("onOrderUpdate: {}", orderUpdate);
    }

    private void onHoldingUpdate(HoldingUpdate holdingUpdate) {
        log.info("onHoldingUpdate: {}", holdingUpdate);
    }

    public void onOpen() {
        log.info("::::PositionFeedClient :: onOpen::::");
    }

    public void onClose(int i, String s) {
        log.info("::::PositionFeedClient :: onClose:::: {}", s);
    }

    public void onError(Throwable e) {
        log.error("::::PositionFeedClient :: ERROR::::", e);
    }

    public void disconnect() {
        log.info("::::PositionFeedClient :: disconnect");
        this.streamer.disconnect();
    }
}
