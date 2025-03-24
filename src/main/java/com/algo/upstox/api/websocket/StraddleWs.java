package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.AlertDto;
import com.algo.upstox.api.model.StraddleMessage;
import com.algo.upstox.common.model.ShortStraddleRequestDto;
import com.algo.upstox.common.model.documents.ShortStraddleDto;
import com.algo.upstox.common.model.documents.StraddleStrikeDto;
import com.algo.upstox.common.model.platform.IndexEnum;
import com.algo.upstox.common.service.StraddleTotalPremiumService;
import com.google.common.util.concurrent.AtomicDouble;
import com.upstox.feeder.MarketUpdateV3;
import com.upstox.feeder.MarketUpdateV3.Feed;
import com.upstox.feeder.MarketUpdateV3.FullFeed;
import com.upstox.feeder.MarketUpdateV3.LTPC;
import com.upstox.feeder.MarketUpdateV3.MarketFullFeed;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.algo.upstox.api.model.AlertDisplayTypeEnum.DANGER_BLINK;
import static com.algo.upstox.api.model.AlertDisplayTypeEnum.INFO_BLINK;
import static com.algo.upstox.common.config.ApplicationContextProvider.getBean;
import static com.algo.upstox.common.model.AlertTypeEnum.CROSSING_DOWN;
import static com.algo.upstox.common.model.AlertTypeEnum.CROSSING_UP;
import static com.algo.upstox.common.model.AlertTypeEnum.INSTRUMENT_COMPARISON;
import static com.algo.upstox.common.model.documents.ShortStraddleStatusEnum.NEW;
import static com.algo.upstox.common.model.documents.ShortStraddleStatusEnum.RUNNING;
import static com.algo.upstox.common.util.AppUtil.currentDateAsString;
import static com.algo.upstox.common.util.AppUtil.formattedDouble;
import static java.util.Objects.isNull;

@Slf4j
public class StraddleWs {
    private final String sessionId;
    private final StraddleTotalPremiumService straddleTotalPremiumService;
    private final WebsocketMessageEmitter websocketMessageEmitter;

    private final Map<IndexEnum, StraddleStrikeDto> straddleMap = new HashMap<>();
    private final StraddleMessage straddleMessage = new StraddleMessage();

    AtomicDouble atomicCeLtp = new AtomicDouble();
    AtomicDouble atomicPeLtp = new AtomicDouble();

    public StraddleWs(String sessionId) {
        this.sessionId = sessionId;
        this.straddleTotalPremiumService = getBean(StraddleTotalPremiumService.class);
        this.websocketMessageEmitter = getBean(WebsocketMessageEmitter.class);
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

    public void runStraddleTotalPremium(MarketUpdateV3 marketData) {
        straddleMap.values().forEach(straddle -> {
            var ce = straddle.getCeInstrument();
            var pe = straddle.getPeInstrument();

            var ceLtp = getLtp(marketData, ce.getInstrumentKey());
            var peLtp = getLtp(marketData, pe.getInstrumentKey());

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

            if (prevSum != 0) {
                straddleMessage.setDiff(newSum - prevSum);
            }

            var hlMap = straddle.getDayHighLowMap();
            if (isNull(hlMap)) {
                straddleTotalPremiumService.updateTodayHighLow(straddle, newSum, newSum);
                setHlMap(straddle);
            } else {
                var hlData = hlMap.get(currentDateAsString());
                if (isNull(hlData)) {
                    straddleTotalPremiumService.updateTodayHighLow(straddle, newSum, newSum);
                    setHlMap(straddle);
                } else {
                    var high = hlData.getHigh();
                    var low = hlData.getLow();
                    if (newSum > high) {
                        straddleTotalPremiumService.updateTodayHighLow(straddle, newSum, low);
                        setHlMap(straddle);
                    }
                    if (low == 0 || newSum < low) {
                        straddleTotalPremiumService.updateTodayHighLow(straddle, high, newSum);
                        setHlMap(straddle);
                    }
                }
            }
//            log.info("Straddle Total Premium {}", straddle.getTotalPremium());
            performTrading(straddle, newSum);
            straddleMessage.setTotalPremium(newSum);
            straddleMessage.getStraddle().setTotalPremium(straddle.getTotalPremium());
            straddleMessage.getStraddle().setDayHighLowMap(straddle.getDayHighLowMap());
            straddle.setTotalPremium(newSum);

            websocketMessageEmitter.emitStraddleLtp(straddleMessage, sessionId);
        });
    }

    void setHlMap(StraddleStrikeDto straddle) {
        straddleTotalPremiumService.getStraddleStrike(sessionId, straddle.getIndex())
                .ifPresent(fetched -> straddle.setDayHighLowMap(fetched.getDayHighLowMap()));
    }

    static double getLtp(MarketUpdateV3 marketData, String instrumentKey) {
        return Optional.ofNullable(marketData)
                .map(MarketUpdateV3::getFeeds)
                .map(map -> map.get(instrumentKey))
                .map(Feed::getFullFeed)
                .map(FullFeed::getMarketFF)
                .map(MarketFullFeed::getLtpc)
                .map(LTPC::getLtp)
                .orElse(0.0);
    }

    private void performTrading(StraddleStrikeDto straddle, double ltp) {
        Predicate<ShortStraddleDto> newTradePredicate = st -> NEW == st.getStatus();
        Predicate<ShortStraddleDto> runningTradePredicate = st -> RUNNING == st.getStatus();

        var hasRunningTrade = shortStraddleStream(straddle).anyMatch(runningTradePredicate);
        var hasNewTrade = shortStraddleStream(straddle).anyMatch(newTradePredicate);

        if (hasNewTrade) {
            shortStraddleStream(straddle).filter(newTradePredicate)
                    .findAny()
                    .ifPresent(ss -> {
                        if (ltp <= ss.getTradePrice()) {
                            log.info("Short straddle entry condition matched");
                            var request = new ShortStraddleRequestDto();
                            request.setEntryPrice(ltp);
                            request.setQuantity(ss.getQuantity());
                            var updated = straddleTotalPremiumService.tradeShortStraddle(sessionId,
                                    straddle.getId(), request);
                            straddle.setShortStraddles(updated.getShortStraddles());
                        }
                    });
        }
        if (hasRunningTrade) {
            shortStraddleStream(straddle).filter(runningTradePredicate)
                    .findAny()
                    .ifPresent(ss -> {
                        if (ltp >= ss.getExitPrice()) {
                            log.info("Short straddle exit condition matched");
                            var request = new ShortStraddleRequestDto();
                            request.setSquaredOffPrice(ltp);
                            request.setQuantity(ss.getQuantity());

                            var updated = straddleTotalPremiumService.exitShortStraddle(sessionId,
                                    straddle.getId(), request);
                            straddle.setShortStraddles(updated.getShortStraddles());
                        }
                    });
        }
    }

    private Stream<ShortStraddleDto> shortStraddleStream(StraddleStrikeDto straddle) {
        return Optional.ofNullable(straddle.getShortStraddles())
                .orElseGet(Collections::emptyList)
                .stream();
    }
}
