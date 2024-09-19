package com.mlorenzo.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@EnableKafka
@Configuration
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        // Por defecto, el modo establecido es de tipo BATCH.
        // Descomentar si queremos probar el modo "MANUAL" de la gestión y envío de los Acks.
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // Si hemos configurado n particiones en un Topic, podemos crear n escuchadores de mensajes(Message Listeners)
        // que se ejecutarán en hilos diferentes. De esta forma, cada Message Listener escuchará los mensajes de una
        // partición en concreto. Esta es una alternativa a crear varias instancias de un consumidor.
        factory.setConcurrency(3);
        // Si no se configura, por defecto se realiza un máximo de 10 reintentos en caso de que haya errores en el
        // procesamiento del mensaje consumido.
        factory.setCommonErrorHandler(buildDefaultErrorHandler());
        return factory;
    }

    private DefaultErrorHandler buildDefaultErrorHandler() {
        // En este caso se configura 2 reintentos como máximo con un intervalo de 1 seg entre cada reintento.
        //DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2));

        // En este caso se configura 2 reintentos como máximo con un intervalo exponencial donde se empieza con un
        // intervalo inicial de 1 seg y se va multiplicando por 2 ese intervalo inicial en cada intervalo siguiente
        // hasta llegar a un máximo de 2 seg.
        ExponentialBackOff exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L);
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setMaxInterval(2000L);
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(exponentialBackOff);

        // Además, se cambia el comportamiento por defecto que se lleva a cabo por cada reintento que se produce
        // por este comportamiento personalizado. Útil para tener más información a la hora de depurar pero no debe
        // usarse en entornos de Producción.
        defaultErrorHandler.setRetryListeners(((record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener, Exception: {}, DeliveryAttempt: {}", ex.getMessage(),
                        deliveryAttempt)));

        // Podemos añadir excepciones(que se consideren por defecto) para que sean ignoradas y no se produzcan
        // reintentos si se producen esas excepciones.
        defaultErrorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        // También podemos añadir excepciones(que no se consideren por defecto) para que se tengan en cuenta
        // para realizar reintentos.
        defaultErrorHandler.addRetryableExceptions(RecoverableDataAccessException.class);

        return defaultErrorHandler;
    }
}
