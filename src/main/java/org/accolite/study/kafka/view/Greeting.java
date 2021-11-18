package org.accolite.study.kafka.view;

import java.io.Serializable;

public record Greeting(String msg, String name) implements Serializable {
}
