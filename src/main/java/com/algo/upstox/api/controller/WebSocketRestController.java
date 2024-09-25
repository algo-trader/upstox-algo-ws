package com.algo.upstox.api.controller;

import com.algo.upstox.api.websocket.WebsocketService;
import com.algo.upstox.model.TaskListDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.algo.upstox.constants.AppConstants.SESSION_ID_HEADER;

@RestController
@RequestMapping("/ws")
@RequiredArgsConstructor
public class WebSocketRestController {
    private final WebsocketService websocketService;

    @PostMapping
    public ResponseEntity<Object> initiate(@RequestHeader(name = SESSION_ID_HEADER, required = false) String sessionId, @RequestBody TaskListDto tasks) {
        websocketService.initiateWebsocket(sessionId, tasks);
        return ResponseEntity.ok().build();
    }
}
