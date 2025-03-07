package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.documents.StraddleStrikeDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.StraddleTotalPremiumService;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.algo.upstox.common.util.AppUtil.formattedDouble;

@Slf4j
public class StraddleWs {
    private final String sessionId;
    private final StraddleTotalPremiumService straddleTotalPremiumService;
    private final Function<Set<String>, String> subscriber;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    private final Map<IndexEnum, StraddleStrikeDto> straddleMap = new HashMap<>();
    private final StraddleMessage straddleMessage = new StraddleMessage();

    public StraddleWs(String sessionId, StraddleTotalPremiumService straddleTotalPremiumService,
                      WebsocketMessageEmitter websocketMessageEmitter, Function<Set<String>, String> subscriber) {
        this.sessionId = sessionId;
        this.straddleTotalPremiumService = straddleTotalPremiumService;
        this.websocketMessageEmitter = websocketMessageEmitter;
        this.subscriber = subscriber;
        fetchStraddle(IndexEnum.NIFTY);
    }

    void fetchStraddle(IndexEnum index) {
        straddleTotalPremiumService.getStraddleStrike(sessionId, index)
                .ifPresent(straddle -> {
                    var subscribeList = Set.of(straddle.getCeInstrument().getInstrumentKey(),
                            straddle.getPeInstrument().getInstrumentKey());
                    subscriber.apply(subscribeList);
                    straddleMessage.setStraddle(straddle);
                    straddleMap.put(index, straddle);
                });
    }

    void deleteStraddle(IndexEnum index) {
        straddleTotalPremiumService.deleteStraddleStrike(sessionId, index);
        straddleMap.remove(index);
    }

    public void runStraddleTotalPremium(FeedResponse response) {
        straddleMap.values().forEach(straddle -> {
            var ce = straddle.getCeInstrument();
            var pe = straddle.getPeInstrument();

            var ceLtp = getLtp(response, ce.getInstrumentKey());
            var peLtp = getLtp(response, pe.getInstrumentKey());

            if (ceLtp == 0 || peLtp == 0) {
                return;
            }
            var sum = formattedDouble(ceLtp + peLtp);
            System.out.println("Straddle Total Premium: " + sum);
            var prevSum = straddleMessage.getTotalPremium();
            straddleMessage.setTotalPremium(sum);
            if (prevSum != 0) {
                straddleMessage.setDiff(sum - prevSum);
            }
            websocketMessageEmitter.emitStraddle(straddleMessage, sessionId);
        });
    }

    private double getLtp(FeedResponse response, String instrumentKey) {
        return Optional.ofNullable(response)
                .map(FeedResponse::getFeedsMap)
                .map(map -> map.get(instrumentKey))
                .map(MarketDataFeed.Feed::getFf)
                .map(MarketDataFeed.FullFeed::getMarketFF)
                .map(MarketDataFeed.MarketFullFeed::getLtpc)
                .map(MarketDataFeed.LTPC::getLtp)
                .orElse(0.0);
    }
}
