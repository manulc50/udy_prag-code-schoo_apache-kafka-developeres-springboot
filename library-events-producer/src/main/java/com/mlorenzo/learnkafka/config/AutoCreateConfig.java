package com.mlorenzo.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

// No se recomienda crear los Topics de este forma, es decir, programáticamente en un entorno de Producción
@Profile("local")
@Configuration
public class AutoCreateConfig {

    @Bean
    public NewTopic libraryEvents() {
        return TopicBuilder.name("library-events")
                .partitions(3)
                .replicas(3)
                .build();
    }
}
