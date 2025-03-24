package com.algo.upstox.api.model;

import lombok.Getter;

@Getter
public enum AlertDisplayTypeEnum {
    DANGER("alert alert-danger"),
    SUCCESS("alert alert-success"),
    WARNING("alert alert-warning"),
    INFO("alert alert-info"),
    DANGER_BLINK("alert alert-danger blink"),
    SUCCESS_BLINK("alert alert-success blink"),
    WARNING_BLINK("alert alert-warning blink"),
    INFO_BLINK("alert alert-info blink");

    private final String classNames;

    AlertDisplayTypeEnum(String classNames) {
        this.classNames = classNames;
    }
}
