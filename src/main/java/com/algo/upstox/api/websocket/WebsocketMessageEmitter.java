package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.AlertDto;
import com.algo.upstox.api.model.EmittedMessages;
import com.algo.upstox.api.model.HoldingDataMessage;
import com.algo.upstox.api.model.LtpcFeed;
import com.algo.upstox.api.model.LtpcFeedMessage;
import com.algo.upstox.api.model.MessageCategoryEnum;
import com.algo.upstox.api.model.PositionDataMessage;
import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.documents.BreakoutTradeDto;
import com.algo.upstox.common.model.documents.SubscriptionDataDto;
import com.algo.upstox.common.model.documents.UserHoldingDto;
import com.algo.upstox.common.model.documents.UserPositionDataDto;
import com.algo.upstox.common.model.documents.UserSubscriptionDto;
import com.algo.upstox.common.model.ws.BidAsk;
import com.algo.upstox.common.model.ws.FeedData;
import com.algo.upstox.common.model.ws.LTPC;
import com.algo.upstox.common.model.ws.OptionGreeks;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.feeder.MarketUpdateV3;
import com.upstox.feeder.MarketUpdateV3.Feed;
import com.upstox.feeder.MarketUpdateV3.IndexFullFeed;
import com.upstox.feeder.MarketUpdateV3.MarketFullFeed;
import com.upstox.feeder.MarketUpdateV3.MarketLevel;
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
    public static final String DESTINATION_EVENT_PUBLISHED = "/topic/messages/event/";
    public static final String DESTINATION_ALERT_TRIGGERED = "/topic/messages/alert-triggered/";
    public static final String DESTINATION_HOLDING_FETCHED = "/topic/messages/holding-fetched/";
    public static final String DESTINATION_HOLDING_SUM = "/topic/messages/holding-sum/";
    public static final String DESTINATION_POSITION_FETCHED = "/topic/messages/position-fetched/";
    public static final String DESTINATION_POSITION_UPDATE = "/topic/messages/position-update/";
    public static final String DESTINATION_OI_DATA = "/topic/messages/oi-data/";

    private static final Map<String, LtpcFeedMessage> ltpMap = new HashMap<>();

    private final SimpMessagingTemplate template;
    private final ObjectMapper objectMapper;

    public void emitTradePlanMessages(List<BreakoutTradeDto> tradePlan, String sessionId) {
        emitMessage(tradePlan, DESTINATION_TP, MessageCategoryEnum.TRADE_PLAN, sessionId);
    }

    public void emitSubscriptionNotify(String sessionId) {
        log.debug("Notifying user for subscription");
        emitMessage("", DESTINATION_SUBSCRIBE_NOTIFY, MessageCategoryEnum.SUBSCRIBE, sessionId);
    }

    private LtpcFeed convertToFeedData(SubscriptionDataDto item, Feed value) {
        return LtpcFeed.builder()
                .index(item.getIndex())
                .tradingSymbol(item.getTradingSymbol())
                .exchange(item.getDisplay().getExchange())
                .expiry(item.getDisplay().getExpiry())
                .fullName(item.getDisplay().getName())
                .feedData(convertToFeedData(value.getFullFeed()))
                .build();
    }

    private FeedData convertToFeedData(MarketUpdateV3.FullFeed fullFeed) {
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

    private List<BidAsk> convertToBidAsk(MarketUpdateV3.MarketLevel marketLevel) {
        return Optional.ofNullable(marketLevel)
                .map(MarketLevel::getBidAskQuote)
                .orElseGet(Collections::emptyList)
                .stream()
                .map(bidAskQuote -> BidAsk.builder()
                        .askPrice(bidAskQuote.getAskP())
                        .askQuantity(bidAskQuote.getAskQ())
                        .bidPrice(bidAskQuote.getBidP())
                        .bidQuantity(bidAskQuote.getBidQ())
                        .build())
                .toList();
    }

    private OptionGreeks convertToOptionGreeks(MarketUpdateV3.OptionGreeks optionGreeks) {
        if (isNull(optionGreeks)) {
            return null;
        }
        return OptionGreeks.builder()
                .delta(formattedDouble(optionGreeks.getDelta()))
                .gamma(formattedDouble(optionGreeks.getGamma()))
                .rho(formattedDouble(optionGreeks.getRho()))
                .theta(formattedDouble(optionGreeks.getTheta()))
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

    public void emitHoldingFetchedMessage(UserHoldingDto holding, String sessionId) {
        emitMessage(holding, DESTINATION_HOLDING_FETCHED, MessageCategoryEnum.HOLDING_FETCHED, sessionId);
    }

    public void emitPositionFetchedMessage(UserPositionDataDto position, String sessionId) {
        emitMessage(position, DESTINATION_POSITION_FETCHED, MessageCategoryEnum.POSITION_FETCHED, sessionId);
    }

    public void emitPositionUpdateMessage(PositionDataMessage position, String sessionId) {
        emitMessage(position, DESTINATION_POSITION_UPDATE, MessageCategoryEnum.POSITION_UPDATE, sessionId);
    }

    public void emitHoldingSumMessage(HoldingDataMessage holding, String sessionId) {
        emitMessage(holding, DESTINATION_HOLDING_SUM, MessageCategoryEnum.HOLDING_SUM_STREAM, sessionId);
    }

    public void emitEventPublished(Object event, String sessionId) {
        emitMessage(event, DESTINATION_EVENT_PUBLISHED, MessageCategoryEnum.EVENT, sessionId);
    }

    public void emitAlertTriggered(String sessionId) {
        emitMessage(null, DESTINATION_ALERT_TRIGGERED, MessageCategoryEnum.ALERT_TRIGGERED, sessionId);
    }

    public void emitOIData(String sessionId, Object oiData) {
        emitMessage(oiData, DESTINATION_OI_DATA, MessageCategoryEnum.OPEN_INTEREST, sessionId);
    }

    private static LTPC buildLtpc(MarketFullFeed marketFullFeed) {
        return LTPC.builder()
                .ltp(marketFullFeed.getLtpc().getLtp())
                .lastTradedQuantity(marketFullFeed.getLtpc().getLtq())
                .lastTradedTime(fromTms(marketFullFeed.getLtpc().getLtt()))
                .closingPrice(marketFullFeed.getLtpc().getCp())
                .build();
    }

    private static boolean isEmpty(IndexFullFeed indexFullFeed) {
        return indexFullFeed == null || indexFullFeed.getLtpc().getLtp() == 0;
    }

    private static boolean isEmpty(MarketUpdateV3.MarketFullFeed marketFullFeed) {
        return marketFullFeed == null || marketFullFeed.getLtpc().getLtp() == 0;
    }

    private static LTPC buildLtpc(IndexFullFeed indexFullFeed) {
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


    public void emitLtpc(UserSubscriptionDto userSubscription, MarketUpdateV3 marketData, String sessionId) {
        List<LtpcFeed> feeds = new ArrayList<>();
        Optional.ofNullable(marketData.getFeeds())
                .ifPresent(map -> {
                    map.forEach((key, value) -> Optional.ofNullable(userSubscription)
                            .map(UserSubscriptionDto::getSubscriptionList)
                            .ifPresent(list -> {
                                list.forEach(item -> {
                                    if (key.equals(item.getInstrumentToken())) {
                                        feeds.add(convertToFeedData(item, value));
                                    }
                                });
                            }));
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
                });

    }
}
