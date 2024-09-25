package com.algo.upstox.api.events;

import com.upstox.marketdatafeeder.rpc.proto.MarketDataFeed.FeedResponse;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class FeedMessageReceivedEvent extends ApplicationEvent {
    private final FeedResponse feedResponse;

    public FeedMessageReceivedEvent(FeedResponse source) {
        super(source);
        this.feedResponse = source;
    }
}
