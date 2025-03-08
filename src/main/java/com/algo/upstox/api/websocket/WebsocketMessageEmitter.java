package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.AlertDto;
import com.algo.upstox.api.model.EmittedMessages;
import com.algo.upstox.api.model.LtpcFeed;
import com.algo.upstox.api.model.LtpcFeedMessage;
import com.algo.upstox.api.model.MessageCategoryEnum;
import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.documents.BreakoutTradeDto;
import com.algo.upstox.common.model.documents.SubscriptionDataDto;
import com.algo.upstox.common.model.documents.UserSubscriptionDto;
import com.algo.upstox.common.model.ws.BidAsk;
import com.algo.upstox.common.model.ws.FeedData;
import com.algo.upstox.common.model.ws.LTPC;
import com.algo.upstox.common.model.ws.OptionGreeks;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.Feed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.MarketFullFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.MarketLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.algo.upstox.common.constants.AppConstants.fromTms;
import static com.algo.upstox.common.util.AppUtil.formattedDouble;
import static java.util.Objects.isNull;

@Component
@AllArgsConstructor
@Slf4j
public class WebsocketMessageEmitter {

    public static final String DESTINATION_TP = "/topic/messages/tp/";
    public static final String DESTINATION_LTPC = "/topic/messages/ltpc/";
    public static final String DESTINATION_STRADDLE = "/topic/messages/straddle/";
    public static final String DESTINATION_STRADDLE_LTP = "/topic/messages/straddle/ltp/";
    public static final String DESTINATION_STRADDLE_ALERT = "/topic/messages/straddle/alert/";
    public static final String DESTINATION_SUBSCRIBE_NOTIFY = "/topic/messages/subscribe/";

    private static final Map<String, LtpcFeedMessage> ltpMap = new HashMap<>();

    private final SimpMessagingTemplate template;
    private final ObjectMapper objectMapper;

    public void emitTradePlanMessages(List<BreakoutTradeDto> tradePlan, String sessionId) {
        emitMessage(tradePlan, DESTINATION_TP, MessageCategoryEnum.TRADE_PLAN, sessionId);
    }

    public void emitSubscriptionNotify(String sessionId) {
        log.info("Notifying user for subscription");
        emitMessage("", DESTINATION_SUBSCRIBE_NOTIFY, MessageCategoryEnum.SUBSCRIBE, sessionId);
    }

    public void emitLtpc(UserSubscriptionDto userSubscription, FeedResponse feedResponse, String sessionId) {
        var map = feedResponse.getFeedsMap();
        List<LtpcFeed> feeds = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        map.forEach((key, value) -> {
            Optional.ofNullable(userSubscription)
                    .map(UserSubscriptionDto::getSubscriptionList)
                    .ifPresent(list -> {
                        list.forEach(item -> {
                            if (key.equals(item.getInstrumentToken())) {
                                feeds.add(convertToFeedData(item, value));
                            }
                        });
                    });
        });
        feeds.sort(Comparator.comparing(LtpcFeed::getIndex));
        var finalList = LtpcFeedMessage.builder()
                .keys(feeds.stream().map(LtpcFeed::getTradingSymbol).toList())
                .feedMap(feeds.stream().collect(Collectors.toMap(LtpcFeed::getTradingSymbol, ltpcFeed -> ltpcFeed)))
                .build();

        if (ltpMap.containsKey(sessionId)) {
            var existing = ltpMap.get(sessionId);
            var existingKeys = existing.getKeys();
            var newList = new ArrayList<String>();
            finalList.getKeys().forEach(key -> {
                newList.add(key);
                if (existingKeys.contains(key)) {
                    existing.getFeedMap().put(key, finalList.getFeedMap().get(key));
                } else {
                    existing.getFeedMap().put(key, finalList.getFeedMap().get(key));
                }
            });
            existing.setKeys(newList);
            ltpMap.put(sessionId, existing);
        } else {
            ltpMap.put(sessionId, finalList);
        }

        emitMessage(ltpMap.get(sessionId), DESTINATION_LTPC, MessageCategoryEnum.LTPC, sessionId);
    }

    private LtpcFeed convertToFeedData(SubscriptionDataDto item, Feed value) {
       return LtpcFeed.builder()
                .index(item.getIndex())
                .tradingSymbol(item.getTradingSymbol())
                .exchange(item.getDisplay().getExchange())
                .expiry(item.getDisplay().getExpiry())
                .fullName(item.getDisplay().getName())
                .feedData(convertToFeedData(value.getFf()))
                .build();
    }

    private FeedData convertToFeedData(MarketDataFeed.FullFeed fullFeed) {
        if (isEmpty(fullFeed.getIndexFF())) {
            if (isEmpty(fullFeed.getMarketFF())) {
                return null;
            }
            return FeedData.builder()
                    .ltpc(buildLtpc(fullFeed.getMarketFF()))
                    .optionGreeks(convertToOptionGreeks(fullFeed.getMarketFF().getOptionGreeks()))
                    .bidAskList(convertToBidAsk(fullFeed.getMarketFF().getMarketLevel()))
                    .build();
        } else {
            return FeedData.builder()
                    .ltpc(buildLtpc(fullFeed.getIndexFF()))
                    .build();
        }
    }

    private List<BidAsk> convertToBidAsk(MarketLevel marketLevel) {
        return Optional.ofNullable(marketLevel)
                .map(MarketLevel::getBidAskQuoteList)
                .orElseGet(Collections::emptyList)
                .stream()
                .map(bidAskQuote -> BidAsk.builder()
                        .askPrice(bidAskQuote.getAp())
                        .askQuantity(bidAskQuote.getAskQ())
                        .bidPrice(bidAskQuote.getBp())
                        .bidQuantity(bidAskQuote.getBidQ())
                        .build())
                .toList();
    }

    private OptionGreeks convertToOptionGreeks(MarketDataFeed.OptionGreeks optionGreeks) {
        if (isNull(optionGreeks)) {
            return null;
        }
        return OptionGreeks.builder()
                .delta(formattedDouble(optionGreeks.getDelta()))
                .gamma(formattedDouble(optionGreeks.getGamma()))
                .rho(formattedDouble(optionGreeks.getRho()))
                .impliedVolatility(formattedDouble(optionGreeks.getIv()))
                .theta(formattedDouble(optionGreeks.getTheta()))
                .underlierPrice(formattedDouble(optionGreeks.getUp()))
                .vega(formattedDouble(optionGreeks.getVega()))
                .build();
    }

    public void emitStraddle(StraddleMessage straddleMessage, String sessionId) {
        emitMessage(straddleMessage, DESTINATION_STRADDLE, MessageCategoryEnum.STRADDLE, sessionId);
    }

    public void emitStraddleLtp(StraddleMessage straddleMessage, String sessionId) {
        emitMessage(straddleMessage, DESTINATION_STRADDLE_LTP, MessageCategoryEnum.STRADDLE, sessionId);
    }

    public void emitAlertMessage(AlertDto alert, String sessionId) {
        emitMessage(alert, DESTINATION_STRADDLE_ALERT, MessageCategoryEnum.STRADDLE, sessionId);
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

    private static boolean isEmpty(MarketDataFeed.MarketFullFeed marketFullFeed) {
        return marketFullFeed == null || marketFullFeed.getLtpc().getLtp() == 0;
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
             log.debug("Emitting message to {} - {} - {}", template.getUserDestinationPrefix(), url, messageBody);
            template.convertAndSend(url, messageBody);
        } catch (JsonProcessingException e) {
            log.error("Unable to process json", e);
        }
    }


}
