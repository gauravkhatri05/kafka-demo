package org.accolite.study.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.accolite.study.kafka.view.Greeting;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
        topics = "#{T(org.accolite.study.kafka.common.constant.AppConstant).TOPIC_NAME.value()}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
        @Payload final String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
      log.info(">>>Message received: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }

    @KafkaListener(
        groupId = "consumer-group-1",
        containerFactory = "kafkaListenerContainerFactory",
        topicPartitions = {
            @TopicPartition(
                topic = "#{T(org.accolite.study.kafka.common.constant.AppConstant).TOPIC_NAME.value()}",
                partitionOffsets = {
                    @PartitionOffset(partition = "0", initialOffset = "0"),  //this will always replay the log for partition '0'
                    @PartitionOffset(partition = "2", initialOffset = "0")  //this will always replay the log for partition '2'
                }
            )
        }
    )
    public void listenToPartition0n2OfGroup1(
        @Payload final String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
        log.info(">>>Message received in listenToPartition0n2OfGroup1: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }

    @KafkaListener(
        groupId = "consumer-group-1",
        containerFactory = "kafkaListenerContainerFactory",
        topicPartitions = {
            @TopicPartition(
                topic = "#{T(org.accolite.study.kafka.common.constant.AppConstant).TOPIC_NAME.value()}",
                partitions = {"2"}
            )
        }
    )
    public void listenToPartition2OfGroup1(
        @Payload final String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
        log.info(">>>Message received in listenToPartition2OfGroup1: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }

    @KafkaListener(
        groupId = "consumer-group-2",
        concurrency = "5",
        containerFactory = "concurrentKafkaListenerContainerFactory",
        topics = "#{T(org.accolite.study.kafka.common.constant.AppConstant).TOPIC_NAME.value()}"
    )
    public void listenToPartitionOfGroup2(
        @Payload final String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
        log.info(">>>Message received in listenToPartitionOfGroup2: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }

    @KafkaListener(
        groupId = "consumer-group-3",
        containerFactory = "filterKafkaListenerContainerFactory",
        topics = "#{T(org.accolite.study.kafka.common.constant.AppConstant).TOPIC_NAME.value()}"
    )
    public void filterListener(
        @Payload final String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
        log.info(">>>Message received in filterListener: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }

    @KafkaListener(
        groupId = "consumer-group-4",
        containerFactory = "greetingKafkaListenerContainerFactory",
        topics = "#{T(org.accolite.study.kafka.common.constant.AppConstant).GREETING_TOPIC_NAME.value()}"
    )
    public void greetingListener(
        @Payload final Greeting message,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
        @Header(KafkaHeaders.OFFSET) final int offset,
        @Header(KafkaHeaders.GROUP_ID) final String groupId
    ) {
        log.info(">>>Message received in greetingListener: '{}' from partition {}, offset {}, and groupId {}.", message, partition, offset, groupId);
    }
}
