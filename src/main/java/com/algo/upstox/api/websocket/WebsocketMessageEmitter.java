package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.EmittedMessages;
import com.algo.upstox.api.model.LtpcFeedMessage;
import com.algo.upstox.api.model.MessageCategoryEnum;
import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.documents.BreakoutTradeDto;
import com.algo.upstox.common.model.documents.UserSubscriptionDto;
import com.algo.upstox.common.model.ws.FeedData;
import com.algo.upstox.common.model.ws.LTPC;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.MarketFullFeed;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.algo.upstox.common.constants.AppConstants.fromTms;

@Component
@AllArgsConstructor
@Slf4j
public class WebsocketMessageEmitter {

    public static final String DESTINATION_TP = "/topic/messages/tp/";
    public static final String DESTINATION_LTPC = "/topic/messages/ltpc/";
    public static final String DESTINATION_STRADDLE = "/topic/messages/straddle/";

    private final SimpMessagingTemplate template;
    private final ObjectMapper objectMapper;

    public void emitTradePlanMessages(List<BreakoutTradeDto> tradePlan, String sessionId) {
        emitMessage(tradePlan, DESTINATION_TP, MessageCategoryEnum.TRADE_PLAN, sessionId);
    }

    public void emitLtpc(UserSubscriptionDto userSubscription, FeedResponse feedResponse, String sessionId) {
        var map = feedResponse.getFeedsMap();
        List<LtpcFeedMessage> feeds = new ArrayList<>();

        map.forEach((key, value) -> {
            Optional.ofNullable(userSubscription)
                    .map(UserSubscriptionDto::getSubscriptionList)
                    .ifPresent(list -> {
                        list.forEach(item -> {
                            if (key.equals(item.getInstrumentToken())) {
                                feeds.add(LtpcFeedMessage.builder()
                                        .index(item.getIndex())
                                        .tradingSymbol(item.getTradingSymbol())
                                        .exchange(item.getDisplay().getExchange())
                                        .expiry(item.getDisplay().getExpiry())
                                        .fullName(item.getDisplay().getName())
                                        .feedData(FeedData.builder()
                                                .instrumentKey(key)
                                                .ltpc(isEmpty(value.getFf().getIndexFF()) ? buildLtpc(value.getFf().getMarketFF()) : buildLtpc(value.getFf().getIndexFF()))
                                                .build())
                                        .build());
                            }
                        });
                    });
        });
        var finalList = feeds.stream().sorted(Comparator.comparing(LtpcFeedMessage::getIndex)).toList();
        emitMessage(finalList, DESTINATION_LTPC, MessageCategoryEnum.LTPC, sessionId);
    }

    public void emitStraddle(StraddleMessage straddleMessage, String sessionId) {
        emitMessage(straddleMessage, DESTINATION_STRADDLE, MessageCategoryEnum.STRADDLE, sessionId);
    }

    public static List<FeedData> toFeedList(FeedResponse response) {
        var map = response.getFeedsMap();
        var feeds = new ArrayList<FeedData>();

        map.forEach((key, value) -> feeds.add(FeedData.builder()
                .instrumentKey(key)
                .ltpc(isEmpty(value.getFf().getIndexFF()) ? buildLtpc(value.getFf().getMarketFF()) : buildLtpc(value.getFf().getIndexFF()))
                .build()));
        return feeds;
    }

    private static LTPC buildLtpc(MarketFullFeed marketFullFeed) {
        return LTPC.builder()
                .ltp(marketFullFeed.getLtpc().getLtp())
                .lastTradedQuantity(marketFullFeed.getLtpc().getLtq())
                .lastTradedTime(fromTms(marketFullFeed.getLtpc().getLtt()))
                .closingPrice(marketFullFeed.getLtpc().getCp())
                .build();
    }

    private static boolean isEmpty(MarketDataFeed.IndexFullFeed indexFullFeed) {
        return indexFullFeed == null || indexFullFeed.getLtpc().getLtp() == 0;
    }

    private static LTPC buildLtpc(MarketDataFeed.IndexFullFeed indexFullFeed) {
        return LTPC.builder()
                .ltp(indexFullFeed.getLtpc().getLtp())
                .lastTradedQuantity(indexFullFeed.getLtpc().getLtq())
                .lastTradedTime(fromTms(indexFullFeed.getLtpc().getLtt()))
                .closingPrice(indexFullFeed.getLtpc().getCp())
                .build();
    }

    private void emitMessage(Object message, String dest, MessageCategoryEnum category, String sessionId) {
        try {
            var url = dest + sessionId;
            var messageBody = objectMapper.writeValueAsString(EmittedMessages.builder()
                    .category(category)
                    .message(message)
                    .build());
            log.info("Emitting message to {} - {} - {} - {}", template.getUserDestinationPrefix(), template.getDefaultDestination(), url, messageBody);
            template.convertAndSend(url, messageBody);
        } catch (JsonProcessingException e) {
            log.error("Unable to process json", e);
        }
    }


}
