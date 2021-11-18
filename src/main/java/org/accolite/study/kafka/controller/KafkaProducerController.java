package org.accolite.study.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.accolite.study.kafka.service.KafkaProducerService;
import org.accolite.study.kafka.view.Greeting;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
@RequiredArgsConstructor
public class KafkaProducerController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(
        @RequestParam(value = "message", required = true) final String message
    ) {
        this.kafkaProducerService.sendMessage(message);
    }

    @PostMapping(value = "/publish-greeting")
    public void sendGreetingMessageToKafkaTopic(
        @RequestBody final Greeting message
    ) {
        this.kafkaProducerService.sendGreetingMessage(message);
    }
}
