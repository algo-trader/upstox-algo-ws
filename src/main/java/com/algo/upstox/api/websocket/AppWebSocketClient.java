package com.algo.upstox.api.websocket;

import com.algo.upstox.common.model.DataObjectDto;
import com.algo.upstox.common.model.SubscriptionRequestDto;
import com.algo.upstox.common.model.documents.SubscriptionDataDto;
import com.algo.upstox.common.model.documents.UserSubscriptionDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.ConditionalTradeService;
import com.algo.upstox.common.service.OrderService;
import com.algo.upstox.common.service.StraddleTotalPremiumService;
import com.algo.upstox.common.service.UserSubscriptionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.algo.upstox.common.constants.AppConstants.METHOD;
import static com.algo.upstox.common.constants.AppConstants.MODE_FULL;
import static com.algo.upstox.common.constants.AppConstants.MODE_OPTION_CHAIN;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final String sessionId;
    private final WebsocketMessageEmitter websocketMessageEmitter;
    private final Predicate<SubscriptionDataDto> nfoInstrumentPredicate = sub -> "NSE_FO".equals(sub.getInstrument().getSegment());

    @Getter
    private final ConditionalTradeWs conditionalTradeWs;

    @Getter
    private final StraddleWs straddleWs;
    private UserSubscriptionDto subscriptions;

    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper,
                              OrderService orderService,
                              String sessionId,
                              WebsocketMessageEmitter websocketMessageEmitter,
                              ConditionalTradeService conditionalTradeService,
                              StraddleTotalPremiumService straddleTotalPremiumService) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.sessionId = sessionId;
        this.websocketMessageEmitter = websocketMessageEmitter;
        this.conditionalTradeWs = new ConditionalTradeWs(sessionId, conditionalTradeService, orderService);
        this.straddleWs = new StraddleWs(sessionId, straddleTotalPremiumService, websocketMessageEmitter);
    }

    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper,
                              String sessionId,
                              WebsocketMessageEmitter websocketMessageEmitter,
                              ConditionalTradeWs conditionalTradeWs,
                              StraddleTotalPremiumService straddleTotalPremiumService) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.sessionId = sessionId;
        this.websocketMessageEmitter = websocketMessageEmitter;
        this.conditionalTradeWs = conditionalTradeWs;
        this.straddleWs = new StraddleWs(sessionId, straddleTotalPremiumService, websocketMessageEmitter);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("Websocket opened {} - {}", serverHandshake.getHttpStatus(), serverHandshake.getHttpStatusMessage());
        sendSubscriptionRequest();
        //doSendSubscriptionRequest(this, constructSubscriptionRequest(Set.of("NSE_FO|45467", "NSE_FO|45469")));
        conditionalTradeWs.fetchRunningTradesOnConnect();
        straddleWs.fetchStraddle(IndexEnum.NIFTY);
    }

    @Override
    public void onMessage(String s) {
    }

    @Override
    public void onMessage(ByteBuffer buffer) {
        log.debug("Received binary message: {}", buffer);
        var response = handleBinaryMessage(buffer);
        websocketMessageEmitter.emitLtpc(subscriptions, response, sessionId);
        conditionalTradeWs.runConditionalTrade(response);
        straddleWs.runStraddleTotalPremium(response);
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        log.info("Closed websocket - {}, {}, {}", i, s, b);
    }

    @Override
    public void onError(Exception e) {
        log.error(e.getMessage(), e);
    }

    private void sendSubscriptionRequest() {
        updateSubscriptions();

        String fullLtpcRequestObject = constructFullSubscriptionRequest();
        byte[] fullLtpcBinaryData = fullLtpcRequestObject.getBytes(StandardCharsets.UTF_8);

        /*String optionChainRequestObject = constructOptionChainSubscriptionRequest();
        byte[] optionChainBinaryData = optionChainRequestObject.getBytes(StandardCharsets.UTF_8);*/

        try {
            send(fullLtpcBinaryData);
        } catch (Exception e) {
            log.error("Unable to request Full LTPC subscription - {} {}",sessionId, e.getMessage());
        }
        /*try {
            send(optionChainBinaryData);
        } catch (Exception e) {
            log.error("Unable to request OptionChain subscription - {} {}", sessionId, e.getMessage());
        }*/
        websocketMessageEmitter.emitSubscriptionNotify(sessionId);
    }

    private String constructFullSubscriptionRequest() {
        return constructFullSubscriptionRequest(subscriptions.getSubscriptionList()
                .stream()
                .map(SubscriptionDataDto::getInstrumentToken)
                .collect(Collectors.toSet()));
    }

    private void updateSubscriptions() {
        this.subscriptions = subscriptionService.retrieveUserSubscriptions(sessionId);
    }

    private String constructFullSubscriptionRequest(Set<String> instrumentTokens) {
        log.info("Sending subscription request - {}", instrumentTokens);
        var subscriptionDetails = SubscriptionRequestDto.builder()
                .guid(UUID.randomUUID().toString())
                .method(METHOD)
                .data(DataObjectDto.builder()
                        .mode(MODE_FULL)
                        .instrumentKeys(instrumentTokens)
                        .build())
                .build();
        try {
            return objectMapper.writeValueAsString(subscriptionDetails);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String constructOptionChainSubscriptionRequest() {
        log.info("Sending OptionChain subscription request");
        var subscriptionDetails = SubscriptionRequestDto.builder()
                .guid(UUID.randomUUID().toString())
                .method(METHOD)
                .data(DataObjectDto.builder()
                        .mode(MODE_OPTION_CHAIN)
                        .instrumentKeys(Optional.ofNullable(subscriptions)
                                .map(UserSubscriptionDto::getSubscriptionList)
                                .orElseGet(TreeSet::new)
                                .stream()
                                .filter(nfoInstrumentPredicate)
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

    static FeedResponse handleBinaryMessage(ByteBuffer bytes) {
        try {
            return FeedResponse.parseFrom(bytes.array());
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage(), e);
            return FeedResponse.getDefaultInstance();
        }
    }
}
