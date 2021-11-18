package org.accolite.study.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.accolite.study.kafka.common.constant.AppConstant;
import org.accolite.study.kafka.view.Greeting;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, Greeting> greetingKafkaTemplate;

    public void sendMessage(final String message) {

        log.info("START: KafKaProducerService.sendMessage is publishing msg {}...", message);

        final ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(AppConstant.TOPIC_NAME.value(), message);

        future.addCallback(

            new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onFailure(final Throwable ex) {
                    log.info("END: KafKaProducerService.sendMessage has failed to publish msg {} due to error {}.",
                        message, ex.getMessage());
                }

                @Override
                public void onSuccess(final SendResult<String, String> result) {
                    log.info("END: KafKaProducerService.sendMessage has successfully published msg {} with offset {}.",
                        message, result.getRecordMetadata().offset());
                }
            }
        );
    }

    public void sendGreetingMessage(final Greeting message) {

        log.info("START: KafKaProducerService.sendGreetingMessage is publishing msg {}...", message);

        final ListenableFuture<SendResult<String, Greeting>> future = this.greetingKafkaTemplate.send(AppConstant.GREETING_TOPIC_NAME.value(), message);

        future.addCallback(

            new ListenableFutureCallback<SendResult<String, Greeting>>() {

                @Override
                public void onFailure(final Throwable ex) {
                    log.info("END: KafKaProducerService.sendGreetingMessage has failed to publish msg {} due to error {}.",
                        message, ex.getMessage());
                }

                @Override
                public void onSuccess(final SendResult<String, Greeting> result) {
                    log.info("END: KafKaProducerService.sendGreetingMessage has successfully published msg {} with offset {}.",
                        message, result.getRecordMetadata().offset());
                }
            }
        );
    }
}
