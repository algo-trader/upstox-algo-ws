package com.algo.upstox.api.websocket;

import com.algo.upstox.common.config.AppPropertyConfig.Scrip;
import com.algo.upstox.common.model.ErrorResponseDto;
import com.algo.upstox.common.model.PlaceOrderResponseDto;
import com.algo.upstox.common.model.conditional.Condition;
import com.algo.upstox.common.model.conditional.ExecutedConditionalTradeDto;
import com.algo.upstox.common.model.conditional.TradeRequest;
import com.algo.upstox.common.model.documents.ConditionalTradeDto;
import com.algo.upstox.common.model.documents.TradeDirectionEnum;
import com.algo.upstox.common.model.platform.OptionsEnum;
import com.algo.upstox.common.service.ConditionalTradeService;
import com.algo.upstox.common.service.OrderService;
import com.upstox.api.PlaceOrderData;
import com.upstox.api.PlaceOrderRequest.ProductEnum;
import com.upstox.api.PlaceOrderRequest.TransactionTypeEnum;
import com.upstox.api.PlaceOrderResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.algo.upstox.common.constants.AppConstants.reverseOf;
import static com.algo.upstox.common.model.conditional.CompareEnum.ABOVE;
import static com.algo.upstox.common.model.conditional.CompareEnum.BELOW;
import static com.algo.upstox.common.util.AppUtil.prepareTradingSymbol;
import static com.algo.upstox.common.util.AppUtil.resolveComparison;
import static java.lang.Integer.parseInt;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@RequiredArgsConstructor
@Slf4j
public class ConditionalTradeWs {
    private final String sessionId;
    private final ConditionalTradeService conditionalTradeService;
    private final OrderService orderService;

    @Setter
    private ConditionalTradeDto conditionalTrade;
    @Setter
    private ExecutedConditionalTradeDto executedTrade;

    public void runConditionalTrade(FeedResponse response) {
        makeEntry(response);
        performExit(response);
    }

    public void loadConditionalTrade() {
        log.info("Fetching conditional trades for user");
        conditionalTradeService.getActiveConditionalTrade(sessionId)
                .ifPresent(this::setConditionalTrade);
    }

    public void updateConditionalTrade(String tradeId) {
        log.info("Fetching running conditional trades for id : Activity : {}", tradeId);
        conditionalTradeService
                .getActiveExecutedTrade(tradeId)
                .ifPresent(this::setExecutedTrade);
    }

    private void makeEntry(FeedResponse response) {
        if (conditionalTrade == null) {
            return;
        }

        if (CollectionUtils.isEmpty(conditionalTrade.getConditions())
                || CollectionUtils.isEmpty(conditionalTrade.getTradeRequests())) {
            return;
        }

        double ltp = getLtp(response, conditionalTrade.getScrip());

        if (ltp < 0) {
            return;
        }
        try {
            boolean isConditionSatisfied = parseConditions(conditionalTrade.getConditions(), conditionalTrade.getScrip(), ltp);
            var condition = conditionalTrade.getConditions().get(0);
            if (isConditionSatisfied) {
                log.info("Conditional trade entry criteria satisfied.");
                var theConditionalTrade = conditionalTrade;
                deactivateConditionalTrade();
                var trades = theConditionalTrade.getTradeRequests();
                var executedTradeResponses = executeTrades(theConditionalTrade.getScrip(), trades);

                var orderIds = executedTradeResponses.stream()
                        .filter(res -> "success".equals(res.getStatus()) || "complete".equals(res.getStatus()))
                        .filter(res -> isNull(res.getError()) && nonNull(res.getResponse()))
                        .map(PlaceOrderResponseDto::getResponse)
                        .map(PlaceOrderResponse::getData)
                        .map(PlaceOrderData::getOrderId)
                        .toList();

                executedTradeResponses
                        .stream().filter(etr -> "failed".equals(etr.getStatus()) || "error".equals(etr.getStatus()))
                        .map(PlaceOrderResponseDto::getError)
                        .filter(Objects::nonNull)
                        .map(ErrorResponseDto::getErrors)
                        .forEach(etr -> etr.forEach(err ->
                                log.info("ORDER FAILED :: [{}] - {}", err.getErrorCode(), err.getMessage())));

                if (CollectionUtils.isEmpty(orderIds)) {
                    return;
                }

                var savedExecutedTrades = conditionalTradeService.saveExecutedConditionalTrade(theConditionalTrade.getScrip(), orderIds, sessionId);
                savedExecutedTrades.setEntryAt(theConditionalTrade.getConditions().get(0).getPrice());
                savedExecutedTrades.setTargetAt(theConditionalTrade.getTargetPrice());
                savedExecutedTrades.setStopLossAt(theConditionalTrade.getStopLossPrice());
                if (BELOW == condition.getCompare()) {
                    savedExecutedTrades.setDirection(TradeDirectionEnum.SHORT);
                } else if (ABOVE == condition.getCompare()) {
                    savedExecutedTrades.setDirection(TradeDirectionEnum.LONG);
                }

                conditionalTradeService.saveExecutedConditionalTrade(savedExecutedTrades);
                setExecutedTrade(savedExecutedTrades);

                log.info("Executed conditional trade saved {}", savedExecutedTrades.getId());

                log.info("Deactivating present conditional trade");
                deactivateConditionalTrade();
            }
        } catch (Exception e) {
            log.error("Error executing conditional trade : {}", e.getMessage());
            deactivateConditionalTrade();
        }
    }

    private void performExit(FeedResponse response) {
        if (executedTrade == null) {
            return;
        }

        var theExecutedTrade = executedTrade;

        var ltp = getLtp(response, theExecutedTrade.getScrip());

        if (ltp < 0) {
            return;
        }

        if (theExecutedTrade.getDirection() == TradeDirectionEnum.SHORT) {
            if (ltp > theExecutedTrade.getStopLossAt()) {
                doCloseTrades(theExecutedTrade);
            }
        }
        if (theExecutedTrade.getDirection() == TradeDirectionEnum.LONG) {
            if (ltp < theExecutedTrade.getStopLossAt()) {
                doCloseTrades(theExecutedTrade);
            }
        }
    }

    private void doCloseTrades(ExecutedConditionalTradeDto theExecutedTrade) {
        List<String> orderIds = new ArrayList<>();
        log.info("Stoploss criteria satisfied");
        executedTrade.getExecutedTrades()
                .forEach(et -> {
                    var lots = et.getQuantity() / parseInt(et.getInstrument().getLotSize());

                    var orderResponse = orderService.placeMarketOrder(et.getInstrument().getTradingSymbol(),
                            lots, reverseOf(et.getTransactionType()),
                            sessionId, Optional.ofNullable(et.getProductType())
                                    .map(pt -> ProductEnum.valueOf(pt.name()))
                                    .orElse(ProductEnum.D));

                    Optional.ofNullable(orderResponse)
                            .map(PlaceOrderResponseDto::getResponse)
                            .map(PlaceOrderResponse::getData)
                            .map(PlaceOrderData::getOrderId)
                            .ifPresent(orderIds::add);
                });

        log.info("Closing executed conditional trade with ID {}", theExecutedTrade.getId());
        conditionalTradeService.closeExecutedConditionalTrades(theExecutedTrade.getId(), orderIds, sessionId);
        setExecutedTrade(null);
    }

    private double getLtp(FeedResponse response, Scrip scrip) {
        return Optional.ofNullable(response)
                .map(FeedResponse::getFeedsMap)
                .map(map -> map.get(scrip.getTradingSymbol()))
                .map(MarketDataFeed.Feed::getFf)
                .map(MarketDataFeed.FullFeed::getIndexFF)
                .map(MarketDataFeed.IndexFullFeed::getLtpc)
                .map(MarketDataFeed.LTPC::getLtp)
                .orElse(-1.0);
    }

    private List<PlaceOrderResponseDto> executeTrades(Scrip scrip, List<TradeRequest> trades) {
        List<PlaceOrderResponseDto> orderResponses = new ArrayList<>();
        trades.forEach(trade -> {
            var tradingSymbol = Optional.ofNullable(trade.getExpiry())
                    .map(exp -> prepareTradingSymbol(scrip.getName(), trade.getStrike(),
                            OptionsEnum.valueOf(trade.getOptionType().name()), exp))
                    .orElseGet(() -> prepareTradingSymbol(scrip, trade.getStrike(),
                            trade.getOptionType().name()));

            var response = orderService.placeMarketOrder(tradingSymbol, trade.getLots(), TransactionTypeEnum.valueOf(trade.getTransactionType().name()),
                    sessionId, Optional.ofNullable(trade.getProduct()).orElse(ProductEnum.D));

            log.info("Order placement status - {} - {}", tradingSymbol, response.getStatus());

            orderResponses.add(response);
        });

        return orderResponses;
    }

    private boolean parseConditions(List<Condition> conditions, Scrip scrip, double ltp) {
        if (ltp < 0) {
            return false;
        }

        var atomicConditions = new AtomicBoolean(true);
        var conditionChecked = new AtomicBoolean(false);

        conditions.forEach(condition -> {
            if (!atomicConditions.get()) {
                return;
            }
            var isSatisfying = resolveComparison(ltp, condition.getCompare(), condition.getPrice(), condition.getDiff());
            if (isSatisfying) {
                log.info("Condition satisfied: {}'s {} is {} {} by {} points.", scrip.getName(), ltp,
                        condition.getCompare(), condition.getPrice(), condition.getDiff());
            }
            atomicConditions.set(atomicConditions.get() && isSatisfying);
            conditionChecked.set(true);
        });

        var finalDecision = conditionChecked.get() && atomicConditions.get();

        log.debug("Final decision: [{}]", finalDecision);
        return finalDecision;
    }

    public void deactivateConditionalTrade() {
        log.info("Deactivating conditional trade");
        setConditionalTrade(null);
        conditionalTradeService.doDeactivateConditionalTrade(sessionId)
                .ifPresent(saved -> log.info("Trade with ID {} deactivated", saved.getId()));
    }

    public void fetchRunningTradesOnConnect() {
        log.info("Fetching running trades and conditional trades");
        conditionalTradeService.getActiveConditionalTrade(sessionId)
                .ifPresent(this::setConditionalTrade);
        conditionalTradeService.getActiveExecutedTradeForUser(sessionId)
                .ifPresent(this::setExecutedTrade);
    }
}
