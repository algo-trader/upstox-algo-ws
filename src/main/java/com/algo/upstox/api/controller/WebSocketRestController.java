package com.algo.upstox.api.controller;

import com.algo.upstox.api.websocket.WebsocketService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.algo.upstox.common.constants.AppConstants.SESSION_ID_HEADER;

@RestController
@RequestMapping("/ws-connect")
@RequiredArgsConstructor
public class WebSocketRestController {
    private final WebsocketService websocketService;

    @PostMapping
    public ResponseEntity<Object> initiate(@RequestHeader(name = SESSION_ID_HEADER, required = false) String sessionId) {
        websocketService.initiateWebsocket(sessionId);
        return ResponseEntity.ok().build();
    }
}
