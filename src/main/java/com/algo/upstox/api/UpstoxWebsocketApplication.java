package com.algo.upstox.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication(scanBasePackages = "com.algo")
@EnableMongoRepositories(basePackages = "com.algo.upstox.repository")
@EnableAsync
public class UpstoxWebsocketApplication{

    public static void main(String[] args) {
        SpringApplication.run(UpstoxWebsocketApplication.class, args);
    }

}
