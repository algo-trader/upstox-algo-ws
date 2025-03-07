package com.algo.upstox.api.websocket;

import com.algo.upstox.common.model.DataObjectDto;
import com.algo.upstox.common.model.SubscriptionRequestDto;
import com.algo.upstox.common.model.documents.SubscriptionDataDto;
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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.algo.upstox.common.constants.AppConstants.METHOD;
import static com.algo.upstox.common.constants.AppConstants.MODE_FULL;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {

    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final String sessionId;

    @Getter
    private final ConditionalTradeWs conditionalTradeWs;
    @Getter
    private final StraddleWs straddleWs;

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
        this.conditionalTradeWs = new ConditionalTradeWs(sessionId, conditionalTradeService, orderService);
        this.straddleWs = new StraddleWs(sessionId, straddleTotalPremiumService, websocketMessageEmitter, this::constructSubscriptionRequest);
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
        this.conditionalTradeWs = conditionalTradeWs;
        this.straddleWs = new StraddleWs(sessionId, straddleTotalPremiumService, websocketMessageEmitter, this::constructSubscriptionRequest);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("Websocket opened {} - {}", serverHandshake.getHttpStatus(), serverHandshake.getHttpStatusMessage());
        sendSubscriptionRequest(this, sessionId);
        doSendSubscriptionRequest(this, constructSubscriptionRequest(Set.of("NSE_FO|45467", "NSE_FO|45469")));
        conditionalTradeWs.fetchRunningTradesOnConnect();
    }

    @Override
    public void onMessage(String s) {
    }

    @Override
    public void onMessage(ByteBuffer buffer) {
        log.debug("Received binary message: {}", buffer);
        var response = handleBinaryMessage(buffer);
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

    private void sendSubscriptionRequest(WebSocketClient client, String sessionId) {
        String requestObject = constructSubscriptionRequest(sessionId);
        byte[] binaryData = requestObject.getBytes(StandardCharsets.UTF_8);
        try {
            client.send(binaryData);
        } catch (Exception e) {
            log.error("Unable to request subscription - {}", e.getMessage());
        }
    }

    private void doSendSubscriptionRequest(WebSocketClient client, String request) {
        byte[] binaryData = request.getBytes(StandardCharsets.UTF_8);
        try {
            client.send(binaryData);
        } catch (Exception e) {
            log.error("Unable to request subscription - {}", e.getMessage());
        }
    }

    private String constructSubscriptionRequest(String sessionId) {
        var subscriptions = subscriptionService.retrieveUserSubscriptions(sessionId);
        return constructSubscriptionRequest(subscriptions.getSubscriptionList()
                .stream()
                .map(SubscriptionDataDto::getInstrumentToken)
                .collect(Collectors.toSet()));
    }

    private String constructSubscriptionRequest(Set<String> instrumentTokens) {
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
        straddleWs.fetchStraddle(index);
    }

    public void deleteStraddleTotalPremium(IndexEnum index) {
        straddleWs.deleteStraddle(index);
    }

    private FeedResponse handleBinaryMessage(ByteBuffer bytes) {
        try {
            return FeedResponse.parseFrom(bytes.array());
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage(), e);
            return FeedResponse.getDefaultInstance();
        }
    }
}
