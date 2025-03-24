package com.algo.upstox.api.websocket;

import com.algo.upstox.common.model.ws.FeedData;
import com.algo.upstox.common.model.ws.LTPC;
import com.algo.upstox.model.ws.FeedData;
import com.algo.upstox.model.ws.LTPC;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.MarketFullFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static com.algo.upstox.common.constants.AppConstants.fromTms;
import static com.algo.upstox.constants.AppConstants.fromTms;

@Component
@Slf4j
@RequiredArgsConstructor
public class FeedMessageEmitter {
    public static final String DESTINATION = "/topic/messages/";

    public void handle(FeedResponse feedResponse, String sessionId) {
        //log.info("{}", event.getFeedResponse());
        var map = feedResponse.getFeedsMap();
        List<FeedData> feeds = new ArrayList<>();

        map.forEach((key, value) -> {
            feeds.add(FeedData.builder()
                    .instrumentKey(key)
                    .ltpc(isEmpty(value.getFf().getIndexFF()) ? buildLtpc(value.getFf().getMarketFF()) : buildLtpc(value.getFf().getIndexFF()))
                    .build());
        });

        /*feeds.stream().filter(f -> Objects.nonNull(f.getInstrumentKey()))
                .forEach(System.out::println);*/
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

}
