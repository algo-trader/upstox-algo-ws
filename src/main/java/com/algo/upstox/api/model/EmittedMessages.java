package com.algo.upstox.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EmittedMessages {
    private MessageCategoryEnum category;
    private Object message;
}
