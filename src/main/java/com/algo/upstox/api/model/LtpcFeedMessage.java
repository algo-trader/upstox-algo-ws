package com.algo.upstox.api.model;

import com.algo.upstox.common.model.ws.FeedData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LtpcFeedMessage {
    private int index;
    private String tradingSymbol;
    private String fullName;
    private String expiry;
    private String exchange;
    private FeedData feedData;
}
