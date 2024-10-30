package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedMessageReceivedEvent;
import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.config.AppPropertyConfig;
import com.algo.upstox.config.AppPropertyConfig.Scrip;
import com.algo.upstox.model.DataObjectDto;
import com.algo.upstox.model.PlaceOrderResponseDto;
import com.algo.upstox.model.SubscriptionRequestDto;
import com.algo.upstox.model.TaskListDto;
import com.algo.upstox.model.documents.BreakoutTradeDto;
import com.algo.upstox.model.documents.TradeDetailsDto;
import com.algo.upstox.model.documents.TradeStatusEnum;
import com.algo.upstox.model.documents.UserPositionDto;
import com.algo.upstox.model.platform.IndexEnum;
import com.algo.upstox.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.upstox.api.OrderData;
import com.upstox.api.PlaceOrderData;
import com.upstox.api.PlaceOrderRequest.ProductEnum;
import com.upstox.api.PlaceOrderResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.Feed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FullFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.IndexFullFeed;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.jetbrains.annotations.NotNull;
import org.springframework.scheduling.annotation.Async;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.algo.upstox.config.AppPropertyConfig.TradeExecutionDirectionEnum.*;
import static com.algo.upstox.constants.AppConstants.*;
import static com.algo.upstox.model.documents.TradeStatusEnum.*;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final FeedResponseEventPublisher feedResponseEventPublisher;
    private final PositionService positionService;
    private final OrderService orderService;
    private final String sessionId;
    private final TaskListDto taskList;
    private final Map<IndexEnum, Scrip> scrips;
    private final BreakoutTradeService breakoutTradeService;

    private final AppPropertyConfig appPropertyConfig;
    private UserPositionDto currentPosition;

    private final AppPropertyConfig.TradeExecutionDirectionEnum tradeExecutionDirection;
    private static final List<TradeStatusEnum> runningTradeStatuses = List.of(EXECUTED, TARGET_1);
    private static final List<TradeStatusEnum> initialTradeStatuses = List.of(PLANNED, MODIFIED);


    @Setter
    private List<BreakoutTradeDto> plannedTrades;

    public List<BreakoutTradeDto> getPlannedTrades() {
        Optional.ofNullable(plannedTrades)
                .orElseGet(() -> {
                    Optional.ofNullable(breakoutTradeService.retrieveAllTrades(sessionId))
                            .ifPresent(this::setPlannedTrades);
                    return plannedTrades;
                });
        log.debug("Planned Trade - {}", plannedTrades);
        return plannedTrades;
    }


    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper, FeedResponseEventPublisher feedResponseEventPublisher,
                              PositionService positionService, OrderService orderService, TaskListDto taskList,
                              String sessionId, Map<IndexEnum, Scrip> scrips, BreakoutTradeService breakoutTradeService,
                              AppPropertyConfig appPropertyConfig) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.feedResponseEventPublisher = feedResponseEventPublisher;
        this.positionService = positionService;
        this.orderService = orderService;
        this.taskList = taskList;
        this.sessionId = sessionId;
        this.scrips = scrips;
        this.breakoutTradeService = breakoutTradeService;
        this.appPropertyConfig = appPropertyConfig;
        tradeExecutionDirection = appPropertyConfig.getPlatform().getTradeExecutionDirection();
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("Websocket opened");
        sendSubscriptionRequest(this, sessionId);
    }

    @Override
    public void onMessage(String s) {
    }

    @Override
    public void onMessage(ByteBuffer buffer) {
        log.debug("Received binary message: {}", buffer);
        var response = handleBinaryMessage(buffer);
        var performingOperation = new AtomicBoolean(false);

        doManageTrade(performingOperation, response);

        feedResponseEventPublisher.publishEvent(new FeedMessageReceivedEvent(response));
    }

    private void doManageTrade(AtomicBoolean performingOperation, MarketDataFeed.FeedResponse response) {
        // Initiate the trade
        getPlannedTrades()
                .stream()
                .filter(filterConditionForNewTrade())
                .forEach(plannedTrade -> {
                    if (!performingOperation.get()) {
                        performingOperation.set(true);
                        var scrip = scrips.get(plannedTrade.getInstrument());
                        var data = response.getFeedsMap().get(scrip.getTradingSymbol());
                        log.debug("Feed data for {} - {}", scrip.getTradingSymbol(), data);
                        verifyAndInitiatePlannedTrade(plannedTrade, data);
                        performingOperation.set(false);
                    }
                });

        // Manage trade
        getPlannedTrades()
                .stream()
                .filter(filterConditionForRunningTrade())
                .forEach(plannedTrade -> {
                    if (!performingOperation.get()) {
                        performingOperation.set(true);
                        var scrip = scrips.get(plannedTrade.getInstrument());
                        var data = response.getFeedsMap().get(scrip.getTradingSymbol());
                        verifyAndExecutePlannedTrades(plannedTrade, data);
                        performingOperation.set(false);
                    }
                });
    }

    private void verifyAndInitiatePlannedTrade(BreakoutTradeDto plannedTrade, MarketDataFeed.Feed data) {
        Optional.ofNullable(data)
                .map(Feed::getFf)
                .map(FullFeed::getIndexFF)
                .map(IndexFullFeed::getLtpc)
                .ifPresent(ltpc -> {
                    if (tradeExecutionDirection == LONG || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> PLANNED == lt.getTradeStatus() || MODIFIED == lt.getTradeStatus())
                                .filter(lt -> ltpc.getLtp() > plannedTrade.getLongAbove())
                                .ifPresent(longTrade -> {
                                    log.info("Long trade plan execution matched");
                                    performTradeInitiation(longTrade, plannedTrade);
                                });
                    }

                    if (tradeExecutionDirection == SHORT || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> PLANNED == st.getTradeStatus())
                                .filter(st -> ltpc.getLtp() < plannedTrade.getShortBelow())
                                .ifPresent(shortTrade -> {
                                    performTradeInitiation(shortTrade, plannedTrade);
                                });
                    }
                });
    }

    private void verifyAndExecutePlannedTrades(BreakoutTradeDto plannedTrade, MarketDataFeed.Feed data) {
        Optional.ofNullable(data)
                .map(Feed::getFf)
                .map(FullFeed::getIndexFF)
                .map(IndexFullFeed::getLtpc)
                .ifPresent(ltpc -> {
                    if (tradeExecutionDirection == LONG || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> EXECUTED == lt.getTradeStatus())
                                .filter(lt -> ltpc.getLtp() < lt.getStopLossAtSpot())
                                .ifPresent(longTrade -> {
                                    log.info("LONG : StopLoss condition has been met, triggering");
                                    performSquareOffForStopLoss(longTrade, plannedTrade);
                                });

                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> EXECUTED == lt.getTradeStatus())
                                .filter(lt -> ltpc.getLtp() > lt.getTarget1AtSpot())
                                .ifPresent(longTrade -> {
                                    log.info("LONG : Target 1 condition met, executing.");
                                    performTargetOrder(longTrade, plannedTrade);
                                });

                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> lt.getAvailableLots() > 0)
                                .filter(lt -> TARGET_1 == lt.getTradeStatus())
                                .ifPresent(longTrade -> {
                                    if (ltpc.getLtp() > longTrade.getTarget2AtSpot()) {
                                        log.info("LONG : Target 2 condition met, executing.");
                                        performTargetOrder(longTrade, plannedTrade);
                                    } else if (ltpc.getLtp() < longTrade.getStopLossAtSpot()) {
                                        log.info("LONG : StopLoss after Target 1, executing.");
                                        performSquareOffForStopLoss(longTrade, plannedTrade);
                                    }
                                });
                    }

                    if (tradeExecutionDirection == SHORT || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> EXECUTED == st.getTradeStatus())
                                .filter(st -> ltpc.getLtp() > st.getStopLossAtSpot())
                                .ifPresent(shortTrade -> {
                                    log.info("SHORT : StopLoss condition has been met, triggering");
                                    performSquareOffForStopLoss(shortTrade, plannedTrade);
                                });

                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> EXECUTED == st.getTradeStatus())
                                .filter(st -> ltpc.getLtp() < st.getTarget1AtSpot())
                                .ifPresent(shortTrade -> {
                                    log.info("SHORT : Target 1 condition met, executing.");
                                    performTargetOrder(shortTrade, plannedTrade);
                                });

                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> TARGET_1 == st.getTradeStatus())
                                .ifPresent(shortTrade -> {
                                    if (ltpc.getLtp() < shortTrade.getTarget2AtSpot()) {
                                        log.info("SHORT : Target 2 condition met, executing.");
                                        performTargetOrder(shortTrade, plannedTrade);
                                    } else if (ltpc.getLtp() > shortTrade.getStopLossAtSpot()) {
                                        log.info("SHORT : StopLoss after Target 1, executing.");
                                        performSquareOffForStopLoss(shortTrade, plannedTrade);
                                    }
                                });
                    }
                });
    }

    private void performTargetOrder(TradeDetailsDto tradeDetails, BreakoutTradeDto plannedTrade) {
        placeTargetOrder(tradeDetails, plannedTrade);
    }

    private void performSquareOffForStopLoss(TradeDetailsDto tradeDetails, BreakoutTradeDto plannedTrade) {
        placeStopLossOrder(tradeDetails, plannedTrade);
    }

    /**
     * Initiate trade
     *
     * @param tradeDetails trade details
     * @param plannedTrade master trade
     */
    private void performTradeInitiation(TradeDetailsDto tradeDetails, BreakoutTradeDto plannedTrade) {
        var orderResponse = placeTradeOrder(tradeDetails);
        runUpdate(plannedTrade, tradeDetails, tradeDetails.getTransactionType().name(),
                tradeDetails.getAvailableLots(), EXECUTED, orderResponse);
    }

    private PlaceOrderResponseDto placeTradeOrder(TradeDetailsDto trade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }
        return orderService.placeMarketOrder(trade.getTradingSymbol(), trade.getAvailableLots(),
                trade.getTransactionType().name(), sessionId, trade.getProduct());
    }

    /**
     * StopLoss trade
     *
     * @param trade        trade details
     * @param plannedTrade master trade
     */
    private void placeStopLossOrder(TradeDetailsDto trade, BreakoutTradeDto plannedTrade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }

        var statusToBeUpdated = EXECUTED == trade.getTradeStatus() ? STOP_LOSS : STOP_LOSS_T1;
        var txnType = reverseOf(trade.getTransactionType().name());

        var order = orderService.placeMarketOrder(trade.getTradingSymbol(), trade.getAvailableLots(),
                txnType, sessionId, trade.getProduct());

        runUpdate(plannedTrade, trade, txnType, trade.getAvailableLots(), statusToBeUpdated, order);
    }

    private void placeTargetOrder(TradeDetailsDto trade, BreakoutTradeDto plannedTrade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }
        TradeStatusEnum statusToBeUpdated;
        int remainingLots = 0;
        int lotsToBook;

        if (EXECUTED == trade.getTradeStatus()) {
            lotsToBook = trade.getLotsToBookAtTarget1();
            remainingLots = trade.getAvailableLots() - lotsToBook;

            if (remainingLots == 0) {
                statusToBeUpdated = SQUARED_OFF_T1;
            } else {
                statusToBeUpdated = TARGET_1;
            }
        } else {
            statusToBeUpdated = SQUARED_OFF_T2;
            lotsToBook = trade.getAvailableLots();
        }

        var txnType = reverseOf(trade.getTransactionType().name());

        var order = orderService.placeMarketOrder(trade.getTradingSymbol(), lotsToBook,
                txnType, sessionId, trade.getProduct());

        runUpdate(plannedTrade, trade, txnType, lotsToBook, statusToBeUpdated, order);

    }

    @Async
    protected void runUpdate(BreakoutTradeDto plannedTrade, TradeDetailsDto trade, String txnType, int lotsToBook,
                             TradeStatusEnum statusToBeUpdated, PlaceOrderResponseDto order) {
        updateTradeDetails(plannedTrade, trade, txnType, lotsToBook, statusToBeUpdated, order);
    }

    private void updateTradeDetails(BreakoutTradeDto plannedTrade, TradeDetailsDto trade, String txnType, int lotsToBook,
                                    TradeStatusEnum statusToBeUpdated, PlaceOrderResponseDto order) {
        log.info("Running update on thread - {}", Thread.currentThread().getName());

        Optional.ofNullable(order.getResponse())
                .map(PlaceOrderResponse::getData)
                .map(PlaceOrderData::getOrderId)
                .ifPresent(orderId -> {
                    try {
                        var orderData = orderService.getCompletedOrder(orderId, sessionId);

                        if (TRANSACTION_TYPE_BUY.equals(txnType)) {
                            var existingAvg = trade.getBuyAvg();
                            log.debug("Calculating new BUY average");
                            trade.setBuyAvg(calculateAverage(trade, lotsToBook, orderData, existingAvg));

                        } else {
                            var existingAvg = trade.getSellAvg();
                            log.debug("Calculating new SELL average");
                            trade.setSellAvg(calculateAverage(trade, lotsToBook, orderData, existingAvg));
                        }
                    } catch (Exception e) {
                        log.error("Unable to fetch order data", e);
                    }
                });
        trade.setAvailableLots(trade.getAvailableLots() - lotsToBook);
        trade.setTradeStatus(statusToBeUpdated);

        if (TARGET_1 == statusToBeUpdated) {
            trade.setStopLossAtSpot(trade.getInitiateAtSpot());
            breakoutTradeService.addToLog(trade, trade.getInitiateAtSpot());
        } else {
            breakoutTradeService.addToLog(trade);
        }

        saveTrade(plannedTrade);
    }

    private double calculateAverage(TradeDetailsDto trade, int lotsToBook, OrderData orderData, double existingAvg) {
        var existingQuantity = trade.getInitialLots() - trade.getAvailableLots();
        var existingTotal = existingAvg * existingQuantity;

        var newAvg = orderData.getAveragePrice();
        var newTotal = newAvg * lotsToBook;

        var finalAvg = (existingTotal + newTotal) / (existingQuantity + lotsToBook);

        log.debug("Average calculation details : existingAvg - {}, existingQuantity - {}, existingTotal - {}, newAvg - {}, newQty - {}, newTotal - {}, finalAvg - {}",
                existingAvg, existingQuantity, existingTotal, newAvg, lotsToBook, newTotal, finalAvg);

        return finalAvg;
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        log.info("Closed websocket");
    }

    @Override
    public void onError(Exception e) {
        log.error(e.getMessage(), e);
    }

    private void sendSubscriptionRequest(WebSocketClient client, String sessionId) {
        String requestObject = constructSubscriptionRequest(sessionId);
        byte[] binaryData = requestObject.getBytes(StandardCharsets.UTF_8);
        log.info("Sending: {}", requestObject);
        client.send(binaryData);
    }

    private String constructSubscriptionRequest(String sessionId) {
        var subscriptions = subscriptionService.retrieveUserSubscriptions(sessionId);

        var subscriptionDetails = SubscriptionRequestDto.builder()
                .guid(UUID.randomUUID().toString())
                .method(METHOD)
                .data(DataObjectDto.builder()
                        .mode(MODE_FULL)
                        .instrumentKeys(subscriptions.getSubscriptionList())
                        .build())
                .build();

        try {
            return objectMapper.writeValueAsString(subscriptionDetails);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FeedResponse handleBinaryMessage(ByteBuffer bytes) {
        try {
            return FeedResponse.parseFrom(bytes.array());

        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage(), e);
            return FeedResponse.getDefaultInstance();
        }
    }

    private void saveTrade(BreakoutTradeDto trade) {
        int index;
        if ((index = getIndexByInstrument(trade.getInstrument())) != -1) {
            log.info("Planned trade found for {} at index {}", trade.getInstrument(), index);
            plannedTrades.set(index, trade);
        }
        breakoutTradeService.updateTrade(trade);
    }

    private int getIndexByInstrument(IndexEnum instrument) {
        for (int i = 0; i < plannedTrades.size(); i++) {
            if (plannedTrades.get(i).getInstrument().equals(instrument)) {
                return i;
            }
        }
        return -1;
    }

    @NotNull
    private static Predicate<BreakoutTradeDto> filterConditionForNewTrade() {
        return prepareCondition(initialTradeStatuses);
    }

    @NotNull
    private static Predicate<BreakoutTradeDto> filterConditionForRunningTrade() {
        return prepareCondition(runningTradeStatuses);
    }

    @NotNull
    private static Predicate<BreakoutTradeDto> prepareCondition(List<TradeStatusEnum> runningTradeStatuses) {
        Predicate<BreakoutTradeDto> longCondition = trade -> Optional.ofNullable(trade.getLongTrade())
                .map(st -> runningTradeStatuses.contains(st.getTradeStatus()))
                .orElse(false);
        Predicate<BreakoutTradeDto> shortCondition = trade -> Optional.ofNullable(trade.getShortTrade())
                .map(st -> runningTradeStatuses.contains(st.getTradeStatus()))
                .orElse(false);

        return longCondition.or(shortCondition);
    }
}
