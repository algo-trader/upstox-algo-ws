package com.algo.upstox.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HoldingDataMessage {
    private double totalValue;
    private double difference;
    private double differencePercentage;
}
