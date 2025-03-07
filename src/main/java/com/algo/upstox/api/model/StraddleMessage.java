package com.algo.upstox.api.model;

import com.algo.upstox.common.model.documents.StraddleStrikeDto;
import lombok.Data;

@Data
public class StraddleMessage {
    private StraddleStrikeDto straddle;
    private double totalPremium;
    private double diff;
}
