package com.algo.upstox.api.websocket;

import com.algo.upstox.api.model.EmittedMessages;
import com.algo.upstox.api.model.MessageCategoryEnum;
import com.algo.upstox.model.documents.BreakoutTradeDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.support.MessageHeaderInitializer;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class WebsocketMessageEmitter {

    public static final String DESTINATION = "/topic/messages/";

    private final SimpMessagingTemplate template;
    private final ObjectMapper objectMapper;

    public void emitTradePlanMessages(List<BreakoutTradeDto> tradePlan, String sessionId) {
        try {
            var url = DESTINATION + sessionId;
            log.info("Emitting message to {} - {} - {}", template.getUserDestinationPrefix(), template.getDefaultDestination(), url);
            template.convertAndSend(url, objectMapper.writeValueAsString(EmittedMessages.builder()
                            .category(MessageCategoryEnum.TRADE_PLAN)
                            .message(tradePlan)
                    .build()));
        } catch (JsonProcessingException e) {
            log.error("Unable to process json", e);
        }
    }

}
