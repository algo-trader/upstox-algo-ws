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
import com.algo.upstox.model.documents.UserPositionDto;
import com.algo.upstox.model.platform.IndexEnum;
import com.algo.upstox.service.BreakoutTradeService;
import com.algo.upstox.service.PlaceOrderService;
import com.algo.upstox.service.PositionService;
import com.algo.upstox.service.UserSubscriptionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.upstox.api.PlaceOrderRequest.ProductEnum;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.Feed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FullFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.IndexFullFeed;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.algo.upstox.config.AppPropertyConfig.TradeExecutionDirectionEnum.*;
import static com.algo.upstox.constants.AppConstants.*;
import static com.algo.upstox.model.documents.TradeStatusEnum.*;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final FeedResponseEventPublisher feedResponseEventPublisher;
    private final PositionService positionService;
    private final PlaceOrderService placeOrderService;
    private final String sessionId;
    private final TaskListDto taskList;
    private final Map<IndexEnum, Scrip> scrips;
    private final BreakoutTradeService breakoutTradeService;

    private final AppPropertyConfig appPropertyConfig;

    private UserPositionDto currentPosition;
    private final AppPropertyConfig.TradeExecutionDirectionEnum tradeExecutionDirection;


    @Setter
    private List<BreakoutTradeDto> plannedTrades;

    public List<BreakoutTradeDto> getPlannedTrades() {
        Optional.ofNullable(plannedTrades)
                .orElseGet(() -> {
                    Optional.ofNullable(breakoutTradeService.retrieveTrade(sessionId))
                            .ifPresent(this::setPlannedTrades);
                    return plannedTrades;
                });
        log.debug("Planned Trade - {}", plannedTrades);
        return plannedTrades;
    }


    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper, FeedResponseEventPublisher feedResponseEventPublisher,
                              PositionService positionService, PlaceOrderService placeOrderService, TaskListDto taskList,
                              String sessionId, Map<IndexEnum, Scrip> scrips, BreakoutTradeService breakoutTradeService,
                              AppPropertyConfig appPropertyConfig) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.feedResponseEventPublisher = feedResponseEventPublisher;
        this.positionService = positionService;
        this.placeOrderService = placeOrderService;
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

        getPlannedTrades()
                .stream()
                .filter(trade -> Optional.ofNullable(trade.getShortTrade())
                        .map(st -> st.getTradeStatus() == PLANNED)
                        .orElse(false))
                .forEach(plannedTrade -> {
                    if (!performingOperation.get()) {
                        performingOperation.set(true);
                        var scrip = scrips.get(plannedTrade.getInstrument());
                        var data = response.getFeedsMap().get(scrip.getTradingSymbol());
                        log.debug("Feed data for {} - {}", scrip.getTradingSymbol(), data);
                        verifyAndExecutePlannedTrade(plannedTrade, data);
                        performingOperation.set(false);
                    }
                });

        getPlannedTrades()
                .stream()
                .filter(trade -> Optional.ofNullable(trade.getShortTrade())
                        .map(st -> st.getTradeStatus() == EXECUTED)
                        .orElse(false))
                .forEach(plannedTrade -> {
                    if (!performingOperation.get()) {
                        performingOperation.set(true);
                        var scrip = scrips.get(plannedTrade.getInstrument());
                        var data = response.getFeedsMap().get(scrip.getTradingSymbol());
                        verifyAndExecuteStopLoss(plannedTrade, data);
                        performingOperation.set(false);
                    }
                });

        feedResponseEventPublisher.publishEvent(new FeedMessageReceivedEvent(response));
    }

    private void verifyAndExecutePlannedTrade(BreakoutTradeDto plannedTrade, MarketDataFeed.Feed data) {
        Optional.ofNullable(data)
                .map(Feed::getFf)
                .map(FullFeed::getIndexFF)
                .map(IndexFullFeed::getLtpc)
                .ifPresent(ltpc -> {
                    if (tradeExecutionDirection == LONG || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> PLANNED == lt.getTradeStatus())
                                .filter(lt -> ltpc.getLtp() > plannedTrade.getLongAbove())
                                .ifPresent(longTrade -> {
                                    log.info("Long trade plan execution matched");
                                    performTradeExecution(longTrade, plannedTrade);
                                });
                    }

                    if (tradeExecutionDirection == SHORT || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> PLANNED == st.getTradeStatus())
                                .filter(st -> ltpc.getLtp() < plannedTrade.getShortBelow())
                                .ifPresent(shortTrade -> {
                                    performTradeExecution(shortTrade, plannedTrade);
                                });
                    }
                });
    }

    private void verifyAndExecuteStopLoss(BreakoutTradeDto plannedTrade, MarketDataFeed.Feed data) {
        Optional.ofNullable(data)
                .map(Feed::getFf)
                .map(FullFeed::getIndexFF)
                .map(IndexFullFeed::getLtpc)
                .ifPresent(ltpc -> {
                    if (tradeExecutionDirection == LONG || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getLongTrade())
                                .filter(lt -> EXECUTED == lt.getTradeStatus())
                                .filter(lt -> ltpc.getLtp() < lt.getStopLossAtSpot())
                                .ifPresent(longTrade -> performSquareOffForStopLoss(longTrade, plannedTrade));
                    }
                    if (tradeExecutionDirection == SHORT || tradeExecutionDirection == BOTH) {
                        Optional.ofNullable(plannedTrade.getShortTrade())
                                .filter(st -> EXECUTED == st.getTradeStatus())
                                .filter(st -> ltpc.getLtp() > st.getStopLossAtSpot())
                                .ifPresent(shortTrade -> performSquareOffForStopLoss(shortTrade, plannedTrade));

                    }
                });
    }

    private void performSquareOffForStopLoss(TradeDetailsDto tradeDetails, BreakoutTradeDto plannedTrade) {
        var orderResponse = placeSquareOffOrder(tradeDetails);

        if (ORDER_PLACEMENT_SUCCESS.equals(orderResponse.getStatus())) {
            tradeDetails.setTradeStatus(SQUARED_OFF);
            saveTrade(plannedTrade);
        }
    }

    private void performTradeExecution(TradeDetailsDto tradeDetails, BreakoutTradeDto plannedTrade) {
        var orderResponse = placeTradeOrder(tradeDetails);
//        var orderResponse = new PlaceOrderResponseDto();
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }
        orderResponse.setStatus(ORDER_PLACEMENT_SUCCESS);
        if (ORDER_PLACEMENT_SUCCESS.equals(orderResponse.getStatus())) {
            tradeDetails.setTradeStatus(EXECUTED);
            saveTrade(plannedTrade);
        }
    }

    private PlaceOrderResponseDto placeTradeOrder(TradeDetailsDto trade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }
        return placeOrderService.placeMarketOrder(trade.getTradingSymbol(), trade.getAvailableLots(),
                trade.getTransactionType().name(), sessionId, trade.getProduct());
    }

    private PlaceOrderResponseDto placeSquareOffOrder(TradeDetailsDto trade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }
        return placeOrderService.placeMarketOrder(trade.getTradingSymbol(), trade.getAvailableLots(),
                reverseOf(trade.getTransactionType().name()), sessionId, trade.getProduct());
       /* var orderResponse = new PlaceOrderResponseDto();
        orderResponse.setStatus(ORDER_PLACEMENT_SUCCESS);
        return orderResponse;*/
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
}
