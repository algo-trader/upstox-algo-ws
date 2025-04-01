package com.algo.upstox.api.websocket;

import com.algo.upstox.common.model.documents.SubscriptionDataDto;
import com.algo.upstox.common.model.documents.UserSubscriptionDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.UserSubscriptionService;
import com.upstox.feeder.MarketDataStreamerV3;
import com.upstox.feeder.MarketUpdateV3;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

import static com.algo.upstox.api.websocket.SessionDataStore.getApiClient;
import static com.algo.upstox.common.config.ApplicationContextProvider.getBean;
import static com.upstox.feeder.constants.Mode.FULL;
import static java.util.Objects.nonNull;

@Slf4j
public class MarketDataFeedV3Client {

    private final UserSubscriptionService subscriptionService;
    private final String sessionId;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    @Getter
    private ConditionalTradeWs conditionalTradeWs;
    @Getter
    private  StraddleWs straddleWs;
    @Getter
    private PortfolioWs portfolioWs;

    private UserSubscriptionDto subscriptions;

    private final MarketDataStreamerV3 streamer;

    public static MarketDataFeedV3Client createMarketDataFeedV3Client(String sessionId) {
        log.info(":::: Creating NEW Market Data Feed V3 Client");
        return new MarketDataFeedV3Client(sessionId);
    }

    private MarketDataFeedV3Client(String sessionId) {
        this.sessionId = sessionId;
        setConditionalTradeWs();
        setStraddleWs();
        setPortfolioWs();

        this.websocketMessageEmitter = getBean(WebsocketMessageEmitter.class);
        this.subscriptionService = getBean(UserSubscriptionService.class);

        this.streamer = initiateClient();
        streamer.connect();
    }

    private MarketDataStreamerV3 initiateClient() {
        var marketDataStreamer = new MarketDataStreamerV3(getApiClient(sessionId),
                subscriptionService.retrieveUserSubscriptions(sessionId)
                        .getSubscriptionList()
                        .stream()
                        .map(SubscriptionDataDto::getInstrumentToken)
                        .collect(Collectors.toSet()), FULL);

        marketDataStreamer.setOnMarketUpdateListener(this::onMessage);
        marketDataStreamer.setOnOpenListener(this::onOpen);
        marketDataStreamer.setOnErrorListener(this::onError);
        marketDataStreamer.setOnCloseListener(this::onClose);
        marketDataStreamer.autoReconnect(true, 2, 15);

        return marketDataStreamer;
    }

    private void setConditionalTradeWs() {
        if (conditionalTradeWs == null) {
            conditionalTradeWs = new ConditionalTradeWs(sessionId);
        }
    }

    private void setStraddleWs() {
        if (straddleWs == null) {
            straddleWs = new StraddleWs(sessionId);
        }
    }

    private void setPortfolioWs() {
        if (portfolioWs == null) {
            portfolioWs = new PortfolioWs(sessionId);
        }
    }

    public void onOpen() {
        log.info(":::: Websocket opened for MarketDataFeedV3Client ::::");
        sendSubscriptionRequest();
        conditionalTradeWs.fetchRunningTradesOnConnect();
        straddleWs.fetchStraddle(IndexEnum.NIFTY);
        portfolioWs.fetchPortfolio(streamer);
    }

    public void onMessage(MarketUpdateV3 marketData) {
        websocketMessageEmitter.emitLtpc(subscriptions, marketData, sessionId);
        conditionalTradeWs.runConditionalTrade(marketData);
        straddleWs.runStraddleTotalPremium(marketData);
        portfolioWs.streamPortfolio(marketData);
    }

    public void onClose(int i, String s) {
        log.info(":::: Closed MarketDataV3Websocket :::: {}, {}", i, s);
        this.streamer.disconnect();
        SessionDataStore.deRegisterMarketDataFeedV3Client(sessionId);
    }

    public void onError(Throwable e) {
        log.error("::: MarketDataV3Websocket Error ::: {}", e.getMessage(), e);
    }

    private void sendSubscriptionRequest() {
        updateSubscriptions();
        try {
            streamer.subscribe(subscriptions.getSubscriptionList()
                    .stream()
                    .map(SubscriptionDataDto::getInstrumentToken)
                    .collect(Collectors.toSet()), FULL);
        } catch (Exception e) {
            log.error("Unable to request Full LTPC subscription - {} {}",sessionId, e.getMessage());
        }
        websocketMessageEmitter.emitSubscriptionNotify(sessionId);
    }

    private void updateSubscriptions() {
        this.subscriptions = subscriptionService.retrieveUserSubscriptions(sessionId);
    }

    public void loadConditionalTrade() {
        conditionalTradeWs.loadConditionalTrade();
    }

    public void updateConditionalTrade(String tradeId) {
        conditionalTradeWs.updateConditionalTrade(tradeId);
    }

    public void deactivateConditionalTrade() {
        conditionalTradeWs.deactivateConditionalTrade();
    }

    public void fetchStraddleTotalPremium(IndexEnum index) {
        log.info("Fetching straddle details - {}", index);
        straddleWs.fetchStraddle(index);
    }

    public void deleteStraddleTotalPremium(IndexEnum index) {
        straddleWs.deleteStraddle(index);
    }

    public void refreshSubscription() {
        sendSubscriptionRequest();
    }

    public void forceCloseTrades() {
        conditionalTradeWs.forceCloseRunningTrades();
    }

    public void disconnect() {
        if (nonNull(streamer)) {
            streamer.disconnect();
        }
    }

    public void fetchPositions() {
        portfolioWs.fetchPositions(streamer);
    }
    public void fetchHoldings() {
        portfolioWs.fetchHoldings(streamer);
    }
}
