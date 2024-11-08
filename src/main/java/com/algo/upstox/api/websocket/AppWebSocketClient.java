package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedMessageReceivedEvent;
import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.config.AppPropertyConfig;
import com.algo.upstox.config.AppPropertyConfig.Scrip;
import com.algo.upstox.model.AddSubscriptionsRequestDto;
import com.algo.upstox.model.DataObjectDto;
import com.algo.upstox.model.PlaceOrderResponseDto;
import com.algo.upstox.model.SubscriptionRequestDto;
import com.algo.upstox.model.documents.BreakoutTradeDto;
import com.algo.upstox.model.documents.SubscriptionDataDto;
import com.algo.upstox.model.documents.TradeDetailsDto;
import com.algo.upstox.model.documents.TradeStatusEnum;
import com.algo.upstox.model.documents.UserSubscriptionDto;
import com.algo.upstox.model.platform.IndexEnum;
import com.algo.upstox.service.BreakoutTradeService;
import com.algo.upstox.service.OrderService;
import com.algo.upstox.service.UserSubscriptionService;
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
import org.springframework.scheduling.annotation.Async;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.algo.upstox.config.AppPropertyConfig.TradeExecutionDirectionEnum.BOTH;
import static com.algo.upstox.config.AppPropertyConfig.TradeExecutionDirectionEnum.LONG;
import static com.algo.upstox.config.AppPropertyConfig.TradeExecutionDirectionEnum.SHORT;
import static com.algo.upstox.constants.AppConstants.METHOD;
import static com.algo.upstox.constants.AppConstants.MODE_FULL;
import static com.algo.upstox.constants.AppConstants.TRANSACTION_TYPE_BUY;
import static com.algo.upstox.constants.AppConstants.reverseOf;
import static com.algo.upstox.model.documents.TradeStatusEnum.EXECUTED;
import static com.algo.upstox.model.documents.TradeStatusEnum.MODIFIED;
import static com.algo.upstox.model.documents.TradeStatusEnum.PLANNED;
import static com.algo.upstox.model.documents.TradeStatusEnum.SQUARED_OFF_T1;
import static com.algo.upstox.model.documents.TradeStatusEnum.SQUARED_OFF_T2;
import static com.algo.upstox.model.documents.TradeStatusEnum.STOP_LOSS;
import static com.algo.upstox.model.documents.TradeStatusEnum.STOP_LOSS_T1;
import static com.algo.upstox.model.documents.TradeStatusEnum.TARGET_1;
import static com.algo.upstox.util.AppUtil.truncateD;
import static com.algo.upstox.util.AppUtil.truncateF;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final String sessionId;
    private final Map<IndexEnum, Scrip> scrips;
    private final BreakoutTradeService breakoutTradeService;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    private final AppPropertyConfig.TradeExecutionDirectionEnum tradeExecutionDirection;

    private static final List<TradeStatusEnum> initialTradeStatuses = List.of(PLANNED, MODIFIED);
    private static final Predicate<TradeDetailsDto> plannedCondition = trade -> initialTradeStatuses.contains(trade.getTradeStatus());
    private static final Predicate<TradeDetailsDto> executedCondition = trade -> EXECUTED == trade.getTradeStatus();
    private static final Predicate<TradeDetailsDto> target1Condition = trade -> TARGET_1 == trade.getTradeStatus();

    private final AtomicBoolean isPlanedTradeFetched = new AtomicBoolean(false);

    @Setter
    private List<BreakoutTradeDto> plannedTrades;
    @Setter
    private UserSubscriptionDto userSubscription;

    public List<BreakoutTradeDto> getPlannedTrades() {
        Optional.ofNullable(plannedTrades)
                .filter(l -> !l.isEmpty())
                .orElseGet(() -> {
                    Optional.ofNullable(breakoutTradeService.retrieveAllTrades(sessionId))
                            .ifPresent(this::setPlannedTrades);

                    websocketMessageEmitter.emitTradePlanMessages(plannedTrades, sessionId);
                    var subscriptionList = new ArrayList<String>();
                    plannedTrades.forEach(pt -> {
                        Optional.ofNullable(pt.getLongTrades())
                                .ifPresent(lt -> subscriptionList.addAll(lt.stream()
                                        .map(TradeDetailsDto::getInstrumentKey).toList()));
                        Optional.ofNullable(pt.getShortTrades())
                                .ifPresent(lt -> subscriptionList.addAll(lt.stream()
                                        .map(TradeDetailsDto::getInstrumentKey).toList()));
                    });
                    subscriptionService.addToUserSubscriptions(sessionId, subscriptionList);
                    setUserSubscription(subscriptionService.retrieveUserSubscriptions(sessionId));
                    this.sendSubscriptionRequest(this, sessionId);
                    return plannedTrades;
                });
        log.debug("Planned Trade - {}", plannedTrades);
        if (!isPlanedTradeFetched.get() && !plannedTrades.isEmpty()) {
            isPlanedTradeFetched.set(true);
            log.info("Planned trade has been fetched..");
        }
        return plannedTrades;
    }


    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper, FeedResponseEventPublisher feedResponseEventPublisher,
                              OrderService orderService,
                              String sessionId, Map<IndexEnum, Scrip> scrips, BreakoutTradeService breakoutTradeService, WebsocketMessageEmitter websocketMessageEmitter,
                              AppPropertyConfig appPropertyConfig) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.orderService = orderService;
        this.sessionId = sessionId;
        this.scrips = scrips;
        this.breakoutTradeService = breakoutTradeService;
        this.websocketMessageEmitter = websocketMessageEmitter;
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
        websocketMessageEmitter.emitLtpc(userSubscription, response, sessionId);
    }

    private void doManageTrade(AtomicBoolean performingOperation, MarketDataFeed.FeedResponse response) {
        // Initiate and manage trades
        getPlannedTrades()
                .forEach(plannedTrade -> {
                    if (!performingOperation.get()) {
                        performingOperation.set(true);
                        var scrip = scrips.get(plannedTrade.getInstrument());
                        var data = response.getFeedsMap().get(scrip.getTradingSymbol());
                        log.debug("Feed data for {} - {}", scrip.getTradingSymbol(), data);

                        verifyAndInitiatePlannedTrade(plannedTrade, data);
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
                        getTradesList(plannedTrade.getLongTrades())
                                .stream()
                                .filter(plannedCondition)
                                .filter(lt -> ltpc.getLtp() > lt.getTriggerAtSpot())
                                .forEach(longTrade -> {
                                    log.info("Long trade plan execution matched");
                                    performTradeInitiation(longTrade, plannedTrade);
                                });
                    }

                    if (tradeExecutionDirection == SHORT || tradeExecutionDirection == BOTH) {
                        getTradesList(plannedTrade.getShortTrades())
                                .stream()
                                .filter(plannedCondition)
                                .filter(st -> ltpc.getLtp() < st.getTriggerAtSpot())
                                .forEach(shortTrade -> {
                                    log.info("Short trade plan execution matched");
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
                        getTradesList(plannedTrade.getLongTrades())
                                .stream()
                                .filter(executedCondition)
                                .forEach(longTrade -> {
                                    if (ltpc.getLtp() < longTrade.getStopLossAtSpot()) {
                                        log.info("LONG : StopLoss condition has been met, triggering");
                                        performSquareOffForStopLoss(longTrade, plannedTrade);
                                    } else if (ltpc.getLtp() > longTrade.getTarget1AtSpot()) {
                                        log.info("LONG : Target 1 condition met, executing.");
                                        performTargetOrder(longTrade, plannedTrade);
                                    }
                                });

                        getTradesList(plannedTrade.getLongTrades())
                                .stream()
                                .filter(target1Condition)
                                .filter(lt -> lt.getAvailableLots() > 0)
                                .forEach(longTrade -> {
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
                        getTradesList(plannedTrade.getShortTrades())
                                .stream()
                                .filter(executedCondition)
                                .forEach(shortTrade -> {
                                    if (ltpc.getLtp() > shortTrade.getStopLossAtSpot()) {
                                        log.info("SHORT : StopLoss condition has been met, triggering");
                                        performSquareOffForStopLoss(shortTrade, plannedTrade);
                                    } else if (ltpc.getLtp() < shortTrade.getTarget1AtSpot()) {
                                        log.info("SHORT : Target 1 condition met, executing.");
                                        performTargetOrder(shortTrade, plannedTrade);
                                    }
                                });
                        getTradesList(plannedTrade.getShortTrades())
                                .stream()
                                .filter(target1Condition)
                                .filter(st -> st.getAvailableLots() > 0)
                                .forEach(shortTrade -> {
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

            log.debug("Lots to book - {}, Remaining Lots - {}", lotsToBook, remainingLots);

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
        trade.setAvailableLots(remainingLots);
        runUpdate(plannedTrade, trade, txnType, lotsToBook, statusToBeUpdated, order);
        log.info("Updated Status - {}", statusToBeUpdated);
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
                        double existingAvg;

                        if (TRANSACTION_TYPE_BUY.equals(txnType)) {
                            existingAvg = trade.getBuyAvg();
                            log.debug("Calculating new BUY average");
                        } else {
                            existingAvg = trade.getSellAvg();
                            log.debug("Calculating new SELL average");
                        }

                        calculateAndSetAverage(trade, lotsToBook, orderData, existingAvg, txnType);

                    } catch (Exception e) {
                        log.error("Unable to fetch order data", e);
                    }
                });

        if (EXECUTED != statusToBeUpdated) {
            trade.setAvailableLots(trade.getAvailableLots() - lotsToBook);
        }

        trade.setTradeStatus(statusToBeUpdated);

        if (TARGET_1 == statusToBeUpdated) {
            trade.setStopLossAtSpot(trade.getInitiateAtSpot());
            breakoutTradeService.addToLog(trade, trade.getInitiateAtSpot());
        } else {
            breakoutTradeService.addToLog(trade);
        }

        saveTrade(plannedTrade);
    }

    private void calculateAndSetAverage(TradeDetailsDto trade, int lotsToBook, OrderData orderData, double existingAvg, String txnType) {
        var existingQuantity = trade.getInitialLots() - trade.getAvailableLots();
        var existingTotal = existingAvg * existingQuantity;

        var newAvg = truncateF(orderData.getAveragePrice());
        var newTotal = truncateF(newAvg * lotsToBook);

        var finalAvg = truncateD((existingTotal + newTotal) / (existingQuantity + lotsToBook));
        var totalTxnValue = truncateF(newTotal * (Optional.ofNullable(scrips.get(trade.getIndex()))
                .map(Scrip::getLotSize)).orElse(0));

        log.info("Average calculation details : existingAvg - {}, existingQuantity - {}, existingTotal - {}, newAvg - {}, newQty - {}, newTotal - {}, finalAvg - {}, totalTxnValue - {}",
                existingAvg, existingQuantity, existingTotal, newAvg, lotsToBook, newTotal, finalAvg, totalTxnValue);

        if (TRANSACTION_TYPE_BUY.equals(txnType)) {
            trade.setTotalBuyValue(totalTxnValue);
            trade.setBuyAvg(finalAvg);
        } else {
            trade.setTotalSellValue(totalTxnValue);
            trade.setSellAvg(finalAvg);
        }
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        log.info("Closed websocket - {}, {}, {}", i, s, b);
    }

    @Override
    public void onError(Exception e) {
        log.error(e.getMessage(), e);
    }

    private void sendSubscriptionRequest(WebSocketClient client, String sessionId) {
        String requestObject = constructSubscriptionRequest(sessionId);
        byte[] binaryData = requestObject.getBytes(StandardCharsets.UTF_8);
        //log.info("Sending: {}", requestObject);
        client.send(binaryData);
    }

    private String constructSubscriptionRequest(String sessionId) {
        var subscriptions = subscriptionService.retrieveUserSubscriptions(sessionId);

        var subscriptionDetails = SubscriptionRequestDto.builder()
                .guid(UUID.randomUUID().toString())
                .method(METHOD)
                .data(DataObjectDto.builder()
                        .mode(MODE_FULL)
                        .instrumentKeys(subscriptions.getSubscriptionList()
                                .stream()
                                .map(SubscriptionDataDto::getInstrumentToken)
                                .collect(Collectors.toSet()))
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

    private List<TradeDetailsDto> getTradesList(List<TradeDetailsDto> longTrades) {
        return Optional.ofNullable(longTrades)
                .orElseGet(ArrayList::new);
    }
}
