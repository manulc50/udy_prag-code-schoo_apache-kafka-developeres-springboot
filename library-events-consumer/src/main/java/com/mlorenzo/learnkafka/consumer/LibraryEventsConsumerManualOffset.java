package com.mlorenzo.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
// Descomentar si queremos probar el modo "MANUAL" de la gestión y envío de los Acks
//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @KafkaListener(topics = "library-events")
    @Override
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord: {}", consumerRecord);
        // Como hemos configurado el modo Manual para la gestión de los Acks, con la llamada a este método,
        // indicamos que hemos procesado el mensaje correctamente y se envía en este momento el Ack al cluster
        // de brokers. Si el modo es de de tipo "BATCH"(modo por defecto), no es debe invocar a este método porque
        // la gestión y envío de los Acks al cluster de brokers se realiza automáticamente por parte del consumidor.
        acknowledgment.acknowledge();
    }
}
