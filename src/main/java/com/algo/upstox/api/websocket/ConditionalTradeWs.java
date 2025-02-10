package com.algo.upstox.api.websocket;

import com.algo.upstox.common.config.AppPropertyConfig.Scrip;
import com.algo.upstox.common.model.PlaceOrderResponseDto;
import com.algo.upstox.common.model.conditional.CompareEnum;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.algo.upstox.common.constants.AppConstants.reverseOf;
import static com.algo.upstox.common.util.AppUtil.prepareTradingSymbol;
import static java.lang.Integer.parseInt;

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

    private void makeEntry(FeedResponse response) {
        conditionalTradeService.getActiveConditionalTrade(sessionId)
                .ifPresent(this::setConditionalTrade);

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

        boolean isConditionSatisfied = parseConditions(conditionalTrade.getConditions(), conditionalTrade.getScrip(), ltp);
        if (isConditionSatisfied) {
            log.info("Conditional trade entry criteria satisfied.");
            var trades = conditionalTrade.getTradeRequests();

            var orderIds = executeTrades(conditionalTrade.getScrip(), trades)
                    .stream().map(PlaceOrderResponseDto::getResponse)
                    .map(PlaceOrderResponse::getData)
                    .map(PlaceOrderData::getOrderId)
                    .toList();

            var savedExecutedTrades = conditionalTradeService.saveExecutedConditionalTrade(conditionalTrade.getScrip(), orderIds, sessionId);
            savedExecutedTrades.setTargetAt(conditionalTrade.getTargetPrice());
            savedExecutedTrades.setStopLossAt(conditionalTrade.getStopLossPrice());
            savedExecutedTrades.setDirection(TradeDirectionEnum.SHORT);

            conditionalTradeService.saveExecutedConditionalTrade(savedExecutedTrades);
            setExecutedTrade(savedExecutedTrades);

            log.info("Executed conditional trade saved {}", savedExecutedTrades.getId());

            log.info("Deactivating present conditional trade");
            deactivateConditionalTrade();
        }
    }

    private void performExit(FeedResponse response) {
        if (executedTrade == null) {
            return;
        }
        conditionalTradeService
                .getActiveExecutedTrade(executedTrade.getId())
                .ifPresent(this::setExecutedTrade);

        var ltp = getLtp(response, executedTrade.getScrip());

        if (ltp < 0) {
            return;
        }

        if (executedTrade.getDirection() == TradeDirectionEnum.SHORT) {
            if (ltp > executedTrade.getStopLossAt()) {
                List<String> orderIds = new ArrayList<>();

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

                log.info("Closing executed conditional trade with ID {}", executedTrade.getId());
                conditionalTradeService.closeExecutedConditionalTrades(executedTrade.getId(), orderIds, sessionId);
                setExecutedTrade(null);
            }
        }
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
            log.info("Condition satisfied: {}'s {} is {} {} by {} points.", scrip.getName(), ltp, condition.getCompare(), condition.getPrice(), condition.getDiff());
            atomicConditions.set(atomicConditions.get() && isSatisfying);
            conditionChecked.set(true);
        });

        var finalDecision = conditionChecked.get() && atomicConditions.get();

        log.debug("Final decision: [{}]", finalDecision);
        return finalDecision;
    }

    private boolean resolveComparison(double ltp, CompareEnum compare, double price, int diff) {
        return switch (compare) {
            case BELOW -> (ltp - diff) <= price;
            case ABOVE -> price >= (ltp + diff);
            case EQUAL -> price == ltp;
            default -> false;
        };
    }

    private boolean resolveComparison(double price1, double price2, CompareEnum compare) {
        return switch (compare) {
            case BELOW -> price1 <= price2;
            case ABOVE -> price1 >= price2;
            case EQUAL -> price1 == price2;
            default -> false;
        };
    }

    private void deactivateConditionalTrade() {
        setConditionalTrade(null);
        conditionalTradeService.deactivateConditionalTrade(sessionId);
    }
}
