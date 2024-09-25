package com.algo.upstox.api.websocket;

import com.algo.upstox.api.events.FeedMessageReceivedEvent;
import com.algo.upstox.api.events.FeedResponseEventPublisher;
import com.algo.upstox.model.DataObjectDto;
import com.algo.upstox.model.SubscriptionRequestDto;
import com.algo.upstox.model.TaskEnum;
import com.algo.upstox.model.TaskListDto;
import com.algo.upstox.model.documents.UserPositionDto;
import com.algo.upstox.service.BreakoutTradeService;
import com.algo.upstox.service.PositionService;
import com.algo.upstox.service.UserSubscriptionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.algo.upstox.constants.AppConstants.METHOD;
import static com.algo.upstox.constants.AppConstants.MODE_FULL;

@Slf4j
public class AppWebSocketClient extends WebSocketClient {
    private final UserSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final FeedResponseEventPublisher feedResponseEventPublisher;
    private final PositionService positionService;
    private final String sessionId;
    private final TaskListDto taskList;

    private UserPositionDto currentPosition;
    private final BreakoutTradeService breakoutTradeService;


    public AppWebSocketClient(URI serverUri, UserSubscriptionService subscriptionService,
                              ObjectMapper objectMapper, FeedResponseEventPublisher feedResponseEventPublisher,
                              PositionService positionService, TaskListDto taskList,
                              String sessionId, BreakoutTradeService breakoutTradeService) {
        super(serverUri);
        this.subscriptionService = subscriptionService;
        this.objectMapper = objectMapper;
        this.feedResponseEventPublisher = feedResponseEventPublisher;
        this.positionService = positionService;
        this.taskList = taskList;
        this.sessionId = sessionId;
        this.breakoutTradeService = breakoutTradeService;
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("Websocket opened");
        sendSubscriptionRequest(this, sessionId);
        if (this.taskList.getTasks().contains(TaskEnum.TRADE)) {
            managePositions(sessionId);
        }
    }

    @Override
    public void onMessage(String s) {
    }

    @Override
    public void onMessage(ByteBuffer buffer) {
        log.info("Received binary message: {}", buffer);
        var response = handleBinaryMessage(buffer);

        breakoutTradeService.retrieveTrade(sessionId)
                .ifPresent(plannedTrade ->);

        /*var data = response.getFeedsMap().get(NIFTY_SCRIP);
        if (NEW == currentPosition.getPositionStatus()) {
            currentPosition.setPositionStatus(BUILDING);
            if (data.getFf().getIndexFF().getLtpc().getLtp() >= currentPosition.getEntryDetails().getLongEntryAt()) {
                log.info("Place long position");
                currentPosition = positionService.addTrade(sessionId, currentPosition.getEntryDetails(), TRADE_TYPE_LONG);
            } else if (data.getFf().getIndexFF().getLtpc().getLtp() <= currentPosition.getEntryDetails().getShortEntryAt()) {
                log.info("Place short position");
                currentPosition = positionService.addTrade(sessionId, currentPosition.getEntryDetails(), TRADE_TYPE_SHORT);
            } else {
                currentPosition.setPositionStatus(NEW);
            }
        }*/
        feedResponseEventPublisher.publishEvent(new FeedMessageReceivedEvent(response));
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        log.info("Closed websocket");
    }

    @Override
    public void onError(Exception e) {
        log.error(e.getMessage(), e);
    }

    private void managePositions(String sessionId) {
        currentPosition = positionService.fetchCurrentUserPosition(sessionId);
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
}
