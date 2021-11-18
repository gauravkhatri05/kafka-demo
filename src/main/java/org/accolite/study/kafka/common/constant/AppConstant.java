package org.accolite.study.kafka.common.constant;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum AppConstant {

    TOPIC_NAME("quickstart-events"),
    GREETING_TOPIC_NAME("greeting-quickstart-events");

    private String value;

    public String value() {
        return this.value;
    }
}
