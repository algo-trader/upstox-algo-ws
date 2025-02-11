package com.algo.upstox.api.config;

import com.algo.upstox.common.config.AppPropertyConfig.PlatformConfig;
import com.algo.upstox.common.model.platform.TradingHolidayDetailsDto;
import com.algo.upstox.common.model.platform.TradingHolidayDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URL;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DataLoader {
    private final PlatformConfig platformConfig;
    private final ObjectMapper objectMapper;

    @Bean("tradingHolidays")
    public List<TradingHolidayDto> holidayList() {
        try {
            return objectMapper.readValue(new URL(platformConfig.getHolidayListUrl()), TradingHolidayDetailsDto.class)
                    .getTradingHolidays();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
