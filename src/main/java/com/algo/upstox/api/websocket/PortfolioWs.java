package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.HoldingDataMessage;
import com.algo.upstox.api.model.PositionDataMessage;
import com.algo.upstox.common.model.documents.HoldingDto;
import com.algo.upstox.common.model.documents.PositionDto;
import com.algo.upstox.common.model.documents.UserHoldingDto;
import com.algo.upstox.common.model.documents.UserPositionDataDto;
import com.algo.upstox.common.service.PortfolioService;
import com.algo.upstox.common.service.UserSubscriptionService;
import com.upstox.feeder.MarketDataStreamerV3;
import com.upstox.feeder.MarketUpdateV3;
import com.upstox.feeder.constants.Mode;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.algo.upstox.api.websocket.StraddleWs.getLtp;
import static com.algo.upstox.common.config.ApplicationContextProvider.getBean;
import static com.algo.upstox.common.util.AppUtil.formattedDouble;
import static java.util.Objects.isNull;
import static org.springframework.util.CollectionUtils.isEmpty;

@Slf4j
public class PortfolioWs {
    private final String sessionId;
    private final PortfolioService portfolioService;
    private final WebsocketMessageEmitter websocketMessageEmitter;
    private final UserSubscriptionService userSubscriptionService;

    private UserHoldingDto userHoldings;
    private final Map<String, Float> holdingLtpMap = new HashMap<>();
    private final Map<String, Float> positionLtpMap = new HashMap<>();
    private UserPositionDataDto userPositions;

    public PortfolioWs(String sessionId) {
        this.sessionId = sessionId;
        this.portfolioService = getBean(PortfolioService.class);
        this.websocketMessageEmitter = getBean(WebsocketMessageEmitter.class);
        this.userSubscriptionService = getBean(UserSubscriptionService.class);
    }

    public void streamPortfolio(MarketUpdateV3 marketUpdate) {
        streamHoldingData(marketUpdate);
        streamPositions(marketUpdate);
    }

    public void fetchPortfolio(MarketDataStreamerV3 streamer) {
        fetchHoldings(streamer);
        fetchPositions(streamer);
    }

    public void fetchHoldings(MarketDataStreamerV3 streamer) {
        this.userHoldings = portfolioService.fetchUserHolding(sessionId);
        Optional.ofNullable(userHoldings)
                .map(UserHoldingDto::getHoldings)
                .filter(list -> !list.isEmpty())
                .ifPresent(list -> {
                    log.info("User holdings found: {}", list);
                    list.forEach(holding -> holdingLtpMap.put(holding.getInstrumentToken(),
                            holding.getClosePrice()));
                    streamer.subscribe(list.stream()
                                    .map(HoldingDto::getInstrumentToken).collect(Collectors.toSet()),
                            Mode.FULL);
                    userSubscriptionService.addToUserSubscriptions(sessionId, list.stream()
                            .map(HoldingDto::getInstrumentToken).toList(), false);
                    websocketMessageEmitter.emitHoldingFetchedMessage(userHoldings, sessionId);
                });
    }

    public void fetchPositions(MarketDataStreamerV3 streamer) {
        this.userPositions = portfolioService.fetchAndUpdateUserPositions(sessionId);
        Optional.ofNullable(userPositions)
                .map(UserPositionDataDto::getPositions)
                .filter(list -> !list.isEmpty())
                .ifPresent(list -> {
                    log.info("User positions found: {}", list);
                    list.forEach(position -> {
                        positionLtpMap.put(position.getInstrumentToken(), position.getAveragePrice());
                        streamer.subscribe(list.stream()
                                .map(PositionDto::getInstrumentToken)
                                .collect(Collectors.toSet()), Mode.FULL);
                        userSubscriptionService.addToUserSubscriptions(sessionId, list.stream()
                                .map(PositionDto::getInstrumentToken).toList(), false);
                        websocketMessageEmitter.emitPositionFetchedMessage(userPositions, sessionId);
                    });
                });
    }

    public void streamPositions(MarketUpdateV3 marketData) {
        if (isNull(userPositions) || isEmpty(userPositions.getPositions())) {
            return;
        }

        userPositions.getPositions().forEach(position -> {
            var ltp = getLtp(marketData, position.getInstrumentToken());
            if (ltp > 0) {
                var quantity = position.getQuantity();
                var pnL = (ltp - (quantity > 0 ? position.getBuyPrice() : position.getSellPrice())) * quantity;
                positionLtpMap.put(position.getInstrumentToken(), Double.valueOf(pnL).floatValue());
            }
        });
        var total = positionLtpMap.values().stream().mapToDouble(f -> f).sum();
        var openPositions = userPositions.getPositions().stream().filter(position -> position.getQuantity() != 0).count();
        var closePositions = userPositions.getPositions().stream().filter(position -> position.getQuantity() == 0).count();
        websocketMessageEmitter.emitPositionUpdateMessage(PositionDataMessage.builder()
                .totalPnL(total)
                .openCount((int) openPositions)
                .closeCount((int) closePositions)
                .positions(userPositions.getPositions())
                .build(), sessionId);
    }

    public void streamHoldingData(MarketUpdateV3 marketData) {
        if (isNull(userHoldings) || isEmpty(userHoldings.getHoldings())) {
            return;
        }

        holdingLtpMap.keySet().forEach(instrumentToken -> {
            double ltp = getLtp(marketData, instrumentToken);
            if (ltp > 0.0) {
                holdingLtpMap.put(instrumentToken, Double.valueOf(ltp).floatValue());
            }
        });

        var sum = userHoldings.getHoldings()
                .stream()
                .mapToDouble(holding -> {
                    var ltp = holdingLtpMap.get(holding.getInstrumentToken());
                    return ltp * holding.getQuantity();
                })
                .sum();

        var diff = sum - userHoldings.getTotalHoldingValue();
        var diffPct = (diff / userHoldings.getTotalHoldingValue()) * 100;

        websocketMessageEmitter.emitHoldingSumMessage(HoldingDataMessage.builder()
                .totalValue(formattedDouble(sum))
                .difference(formattedDouble(diff))
                .differencePercentage(formattedDouble(diffPct))
                .build(), sessionId);
    }
}
