package com.mlorenzo.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mlorenzo.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendLibraryEventAsync(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // El método "sendDefault" enviará el evento al Topic establecido por defecto en el KafkaTemplate en el archivo
        // de propiedades. La llamada a este método es asíncrona y se ejecuta en otro hilo distinto al principal.
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        // Callback que se ejecutará cuando finalice el envío del evento a Kafka
        listenableFuture.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventAsyncWithSend(LibraryEvent libraryEvent)
            throws JsonProcessingException {
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        // A diferencia del método "sendDefault", en el método "send" tenemos que indicar el Topic explícitamente.
        // La llamada a este método es asíncrona y se ejecuta en otro hilo distinto al principal.
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate
                //.send("library-events", key, value);
                // Otra forma de enviar un evento usando el método "send" a partir de un objeto de tipo ProducerRecord
                .send(buildProducerrecord(key, value, "library-events"));
        // Callback que se ejecutará cuando finalice el envío del evento a Kafka
        listenableFuture.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

        return listenableFuture;
    }

    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            // En este caso, llamando al método "get", estamos bloqueando la llamada al método "sendDefault"
            // haciendo que la llamada sea síncrona y se ejecute en el mismo hilo que el principal
            sendResult = kafkaTemplate.sendDefault(key, value).get();
            // Podemos indicar un timeout de respuesta
            //sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException e) {
            handleFailure(key, value, e);
        }
        return sendResult;
    }

    // Enviar eventos usando objetos de tipo ProducerRecord nos permite la posibilidad de enviar también nuestras
    // propias cabeceras
    private ProducerRecord<Integer, String> buildProducerrecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key: {} and the value: {}. Partition is: {}", key, value,
                result.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message for the key: {} and the value: {}. The exception is: {}", key, value,
                ex.getMessage());
    }
}
