package com.algo.upstox.api.config;

import com.algo.upstox.config.AppPropertyConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "app.config")
public class AppConfig extends AppPropertyConfig {
}
