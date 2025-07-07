package com.algo.upstox.api.model;

import com.algo.upstox.common.model.documents.InstrumentDto;
import com.algo.upstox.common.model.documents.PositionDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PositionDataMessage {
    private int openCount;
    private int closeCount;
    private double totalPnL;
    private double dayHigh;
    private double dayLow;
    private List<PositionDto> positions;
}
