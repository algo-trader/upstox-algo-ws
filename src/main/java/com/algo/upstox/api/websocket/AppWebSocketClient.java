package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.common.config.AppPropertyConfig.Scrip;
import com.algo.upstox.common.model.DataObjectDto;
import com.algo.upstox.common.model.PlaceOrderResponseDto;
import com.algo.upstox.common.model.SubscriptionRequestDto;
import com.algo.upstox.common.model.conditional.CompareEnum;
import com.algo.upstox.common.model.conditional.Condition;
import com.algo.upstox.common.model.conditional.TradeRequest;
import com.algo.upstox.common.model.documents.ConditionalTradeDto;
import com.algo.upstox.common.model.documents.SubscriptionDataDto;
import com.algo.upstox.common.model.documents.TradeDetailsDto;
import com.algo.upstox.common.model.documents.TradeStatusEnum;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.model.platform.OptionsEnum;
import com.algo.upstox.common.service.ConditionalTradeService;
import com.algo.upstox.common.service.OrderService;
import com.algo.upstox.common.service.UserSubscriptionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.InvalidProtocolBufferException;
import com.upstox.api.PlaceOrderRequest;
import com.upstox.api.PlaceOrderRequest.ProductEnum;
import com.upstox.api.PlaceOrderRequest.TransactionTypeEnum;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.util.CollectionUtils;

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

import static com.algo.upstox.common.constants.AppConstants.METHOD;
import static com.algo.upstox.common.constants.AppConstants.MODE_FULL;
import static com.algo.upstox.common.constants.AppConstants.reverseOf;
import static com.algo.upstox.common.model.documents.TradeStatusEnum.EXECUTED;
import static com.algo.upstox.common.model.documents.TradeStatusEnum.MODIFIED;
import static com.algo.upstox.common.model.documents.TradeStatusEnum.PLANNED;
import static com.algo.upstox.common.model.documents.TradeStatusEnum.TARGET_1;
import static com.algo.upstox.common.util.AppUtil.prepareTradingSymbol;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final String sessionId;
    private final Map<IndexEnum, Scrip> scrips;
    private final WebsocketMessageEmitter websocketMessageEmitter;
    private final ConditionalTradeService conditionalTradeService;

    private final ConditionalTradeWs conditionalTradeWs;

    private static final List<TradeStatusEnum> initialTradeStatuses = List.of(PLANNED, MODIFIED);
    private static final Predicate<TradeDetailsDto> plannedCondition = trade -> initialTradeStatuses.contains(trade.getTradeStatus());
    private static final Predicate<TradeDetailsDto> executedCondition = trade -> EXECUTED == trade.getTradeStatus();
    private static final Predicate<TradeDetailsDto> target1Condition = trade -> TARGET_1 == trade.getTradeStatus();



    @Setter
    private ConditionalTradeDto conditionalTrade;
    @Setter
    private double conditionalTradeTargetPrice;
    @Setter
    private double conditionalTradeEntryPrice;
    @Setter
    double conditionalTradeStopLossPrice;
    @Setter
    boolean isConditionalTradeExecuted = false;
    @Setter
    private CompareEnum conditionalTradeCompare;
    @Setter
    private Scrip conditionalTradeScrip;

    private final List<TradeRequest> reverseTrades = new ArrayList<>();

    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper, FeedResponseEventPublisher feedResponseEventPublisher,
                              OrderService orderService,
                              String sessionId, Map<IndexEnum, Scrip> scrips, WebsocketMessageEmitter websocketMessageEmitter, ConditionalTradeService conditionalTradeService) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.orderService = orderService;
        this.sessionId = sessionId;
        this.scrips = scrips;
        this.websocketMessageEmitter = websocketMessageEmitter;
        this.conditionalTradeService = conditionalTradeService;
        this.conditionalTradeWs = new ConditionalTradeWs(sessionId, conditionalTradeService, orderService);
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
        var atomicLtp = new AtomicDouble();

        conditionalTradeWs.runConditionalTrade(response);

        /*if (isConditionalTradeExecuted) {
            var ltp = getLtp(response, conditionalTradeScrip);

            if (ltp > 0 && conditionalTradeEntryPrice > 0 && resolveComparison(ltp, conditionalTradeEntryPrice, conditionalTradeCompare)) {
                executeTrades(conditionalTradeScrip, reverseTrades);
            }
        }*/
    }

    private PlaceOrderResponseDto placeTradeOrder(TradeDetailsDto trade) {
        if (Objects.isNull(trade.getProduct())) {
            trade.setProduct(ProductEnum.I);
        }
        return orderService.placeMarketOrder(trade.getTradingSymbol(), trade.getAvailableLots(),
                PlaceOrderRequest.TransactionTypeEnum.fromValue(trade.getTransactionType().name()), sessionId, trade.getProduct());
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
        try {
            client.send(binaryData);
        } catch (Exception e) {
            log.error("Unable to request subscription - {}", e.getMessage());
        }
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

    private void forceFetchConditionalTrade() {
        setConditionalTrade(null);
        conditionalTradeService.getActiveConditionalTrade(sessionId)
                .ifPresent(this::setConditionalTrade);
    }



}
