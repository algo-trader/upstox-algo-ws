package com.algo.upstox.api;

import com.algo.upstox.model.documents.LoggedInUserDto;
import com.algo.upstox.service.AuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class WebSocketLoggedInUserService {
    private final AuthService authService;

    public void updateSessionDetails(String sessionId, String simpSessionId) {
        Optional.ofNullable(authService.getLoggedInUser(sessionId))
                .ifPresent(u -> {
                    log.info("Saving simp sessionId to user {}", u.getUserName());
                    u.setSimpSessionId(simpSessionId);
                    authService.saveUserSessionDetails(u);
                });
    }

    public Optional<String> retrieveSessionIdBySimpSessionId(String simpSessionId) {
        return Optional.ofNullable(authService.getLoggedInUserBySimpSessionId(simpSessionId))
                .map(LoggedInUserDto::getSessionId);
    }
}
