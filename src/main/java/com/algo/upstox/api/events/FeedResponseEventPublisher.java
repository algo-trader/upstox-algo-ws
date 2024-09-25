package com.algo.upstox.api.events;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class FeedResponseEventPublisher {
    private final ApplicationEventPublisher applicationEventPublisher;

    public void publishEvent(FeedMessageReceivedEvent event) {
        //log.info("Publishing event {}", event);
        applicationEventPublisher.publishEvent(event);
    }
}
