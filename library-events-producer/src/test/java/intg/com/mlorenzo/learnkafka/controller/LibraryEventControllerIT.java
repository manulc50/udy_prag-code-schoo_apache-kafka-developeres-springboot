package com.mlorenzo.learnkafka.controller;

import com.mlorenzo.learnkafka.domain.Book;
import com.mlorenzo.learnkafka.domain.LibraryEvent;
import com.mlorenzo.learnkafka.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Para lanzar las pruebas de integración sobre un Kafka embebido en memoria en vez del Kafka real
@EmbeddedKafka(topics = "library-events", partitions = 3)
// Sobrescribimos estas propiedades para indicar las localizaciones de los brokers de Kafka embebidos en memoria en
// vez de las localizaciones de los brokers reales
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventControllerIT {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils
                .consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // Como el productor envía el evento de forma asíncrona, con esta anotación establecemos un timeout en la ejecución
    // de este test para que el consumidor tenga tiempo de consumir el evento y, así,  poder realizar la
    // comprobaciones correctamente.
    @Timeout(5)
    @Test
    void postLibraryEventAsyncWithSendTest() {
        // given
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
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, httpHeaders);

        // when
        ResponseEntity<Void> responseEntity = testRestTemplate
                .exchange("/v1/library-events/async/send", HttpMethod.POST, request, Void.class);

        // then
        assertEquals(HttpStatus.ACCEPTED, responseEntity.getStatusCode());
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils
                .getSingleRecord(consumer, "library-events");
        String expectedValue = "{\"id\":null,\"book\":{\"id\":123,\"name\":\"Kafka using Spring Boot\","
                + "\"author\":\"John Doe\"},\"type\":\"NEW\"}";
        assertEquals(expectedValue, consumerRecord.value());
    }

    @Test
    @Timeout(5)
    void putLibraryEventAsyncWithSendTest() throws InterruptedException {
        //given
        Book book = Book.builder()
                .id(456)
                .author("John Doe")
                .name("Kafka using Spring Boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(123)
                .book(book)
                .build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        //when
        ResponseEntity<LibraryEvent> responseEntity = testRestTemplate
                .exchange("/v1/library-events", HttpMethod.PUT, request, LibraryEvent.class);

        //then
        assertEquals(HttpStatus.ACCEPTED, responseEntity.getStatusCode());
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String expectedValue = "{\"id\":123,\"book\":{\"id\":456,\"name\":\"Kafka using Spring Boot\","
            + "\"author\":\"John Doe\"},\"type\":\"UPDATE\"}";
        assertEquals(expectedValue, consumerRecord.value());
    }
}
