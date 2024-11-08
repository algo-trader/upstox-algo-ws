package com.algo.upstox.api.model;

import com.algo.upstox.model.ws.FeedData;
import com.upstox.feeder.MarketUpdate.LTPC;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LtpcFeedMessage {
    private String tradingSymbol;
    private FeedData feedData;
}
