package com.algo.upstox.api.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckRestController {
    @GetMapping("/health")
    public ResponseEntity<Object> healthCheck() {
        return ResponseEntity.ok().build();
    }
}
