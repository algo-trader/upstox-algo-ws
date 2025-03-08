package com.algo.upstox.api.model;

import com.algo.upstox.common.model.AlertTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AlertDto {
    private String alertText;
    private AlertTypeEnum alertType;
    private String alertDisplayType;
    private long timeout;
    private boolean isAudioOn;
}
