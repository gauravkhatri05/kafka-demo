package org.accolite.study.kafka.config;

import org.accolite.study.kafka.common.constant.AppConstant;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin kafkaAdmin(
        @Value("${spring.kafka.bootstrap-servers}") final String bootstrapServers
    ) {
        return new KafkaAdmin(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        );
    }

    @Bean
    public NewTopic newTopic() {
        return new NewTopic(AppConstant.TOPIC_NAME.value(), 3, (short) 3);
    }

    @Bean
    public NewTopic newGreetingTopic() {
        return new NewTopic(AppConstant.GREETING_TOPIC_NAME.value(), 3, (short) 3);
    }
}
