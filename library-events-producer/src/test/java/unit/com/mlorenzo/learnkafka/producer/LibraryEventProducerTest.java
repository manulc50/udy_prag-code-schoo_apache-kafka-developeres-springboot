package com.mlorenzo.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mlorenzo.learnkafka.domain.Book;
import com.mlorenzo.learnkafka.domain.LibraryEvent;
import com.mlorenzo.learnkafka.domain.LibraryEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper;

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    void testSendLibraryEventAsyncWithSendFailure() throws JsonProcessingException, ExecutionException,
            InterruptedException {
        //given
        Book book = Book.builder()
                .id(123)
                .name("Kafka using Spring Boot")
                .author("John Doe")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .type(LibraryEventType.NEW)
                .build();
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        listenableFuture.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);

        //when
        assertThrows(ExecutionException.class, () ->
            libraryEventProducer.sendLibraryEventAsyncWithSend(libraryEvent).get()
        );
    }

    @Test
    void testSendLibraryEventAsyncWithSendSuccess() throws JsonProcessingException, ExecutionException,
            InterruptedException {
        //given
        Book book = Book.builder()
                .id(123)
                .name("Kafka using Spring Boot")
                .author("John Doe")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .type(LibraryEventType.NEW)
                .build();
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        String record = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events",
                libraryEvent.getId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1, 1, 342, System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
        listenableFuture.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);

        //when
        ListenableFuture<SendResult<Integer, String>> listenableFutureResponse = libraryEventProducer
                .sendLibraryEventAsyncWithSend(libraryEvent);

        //then
        SendResult<Integer, String> sendResultResponse = listenableFutureResponse.get();
        assertEquals(1, sendResultResponse.getRecordMetadata().partition());
    }
}
