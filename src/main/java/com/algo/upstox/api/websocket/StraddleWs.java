package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.AlertDto;
import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.documents.StraddleStrikeDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.StraddleTotalPremiumService;
import com.google.common.util.concurrent.AtomicDouble;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.algo.upstox.api.model.AlertDisplayTypeEnum.DANGER_BLINK;
import static com.algo.upstox.api.model.AlertDisplayTypeEnum.INFO_BLINK;
import static com.algo.upstox.common.model.AlertTypeEnum.CROSSING_DOWN;
import static com.algo.upstox.common.model.AlertTypeEnum.CROSSING_UP;
import static com.algo.upstox.common.model.AlertTypeEnum.INSTRUMENT_COMPARISON;
import static com.algo.upstox.common.util.AppUtil.formattedDouble;

@Slf4j
public class StraddleWs {
    private final String sessionId;
    private final StraddleTotalPremiumService straddleTotalPremiumService;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    private final Map<IndexEnum, StraddleStrikeDto> straddleMap = new HashMap<>();
    private final StraddleMessage straddleMessage = new StraddleMessage();

    AtomicDouble atomicCeLtp = new AtomicDouble();
    AtomicDouble atomicPeLtp = new AtomicDouble();

    public StraddleWs(String sessionId, StraddleTotalPremiumService straddleTotalPremiumService,
                      WebsocketMessageEmitter websocketMessageEmitter) {
        this.sessionId = sessionId;
        this.straddleTotalPremiumService = straddleTotalPremiumService;
        this.websocketMessageEmitter = websocketMessageEmitter;
    }

    void fetchStraddle(IndexEnum index) {
        straddleTotalPremiumService.getStraddleStrike(sessionId, index)
                .ifPresent(straddle -> {
                    straddleMessage.setStraddle(straddle);
                    straddleMap.remove(index);
                    straddleMap.put(index, straddle);
                    websocketMessageEmitter.emitStraddle(straddleMessage, sessionId);
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

            if (ceLtp == 0 && peLtp == 0) {
                return;
            }

            var prevSum = straddleMessage.getTotalPremium();

            if (ceLtp > 0) {
                atomicCeLtp.set(ceLtp);

            }
            if (peLtp > 0) {
                atomicPeLtp.set(peLtp);
            }

            var newSum = formattedDouble(atomicCeLtp.get() + atomicPeLtp.get());

            if (straddle.getTotalPremiumUpperRange() > 0
                    && !straddle.isUpperRangeAlertTriggered()
                    && newSum > straddle.getTotalPremiumUpperRange()) {
                var message = String.format("Total premium price '%s' is greater than upper limit price '%s'!!",
                        newSum, formattedDouble(straddle.getTotalPremiumUpperRange()));
                straddleTotalPremiumService.triggerAlert(sessionId, straddle.getIndex(), CROSSING_UP);
                straddle.setUpperRangeAlertTriggered(true);
                websocketMessageEmitter.emitAlertMessage(AlertDto.builder()
                        .alertText(message)
                        .alertType(CROSSING_UP)
                        .alertDisplayType(DANGER_BLINK.getClassNames())
                        .isAudioOn(true)
                        .timeout(15000)
                        .build(), sessionId);
            }

            if (straddle.getTotalPremiumLowerRange() > 0
                    && !straddle.isLowerRangeAlertTriggered()
                    && newSum < straddle.getTotalPremiumLowerRange()) {
                var message = String.format("Total premium price '%s' is lesser than lower limit price '%s'!!",
                        newSum, formattedDouble(straddle.getTotalPremiumLowerRange()));
                log.info("{} - {} - {}", message, ceLtp, peLtp);
                straddleTotalPremiumService.triggerAlert(sessionId, straddle.getIndex(), CROSSING_DOWN);
                straddle.setLowerRangeAlertTriggered(true);
                websocketMessageEmitter.emitAlertMessage(AlertDto.builder()
                        .alertText(message)
                        .alertType(CROSSING_DOWN)
                        .alertDisplayType(INFO_BLINK.getClassNames())
                        .isAudioOn(true)
                        .timeout(15000)
                        .build(), sessionId);
            }

            if (straddle.isInstrumentComparisonEnabled()) {
                if (atomicCeLtp.get() / atomicPeLtp.get() > straddle.getInstrumentComparisonMultiplier()
                        || atomicPeLtp.get() / atomicCeLtp.get() > straddle.getInstrumentComparisonMultiplier()) {
                    straddleTotalPremiumService.triggerAlert(sessionId, straddle.getIndex(), INSTRUMENT_COMPARISON);
                }
            }

//            log.info("Straddle Total Premium {}", straddle.getTotalPremium());
            straddleMessage.setTotalPremium(newSum);
            straddleMessage.getStraddle().setTotalPremium(straddle.getTotalPremium());
            straddle.setTotalPremium(newSum);

            if (prevSum != 0) {
                straddleMessage.setDiff(newSum - prevSum);
            }
            websocketMessageEmitter.emitStraddleLtp(straddleMessage, sessionId);
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
