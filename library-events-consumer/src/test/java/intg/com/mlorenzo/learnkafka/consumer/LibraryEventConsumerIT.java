package com.mlorenzo.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mlorenzo.learnkafka.entity.Book;
import com.mlorenzo.learnkafka.entity.LibraryEvent;
import com.mlorenzo.learnkafka.entity.LibraryEventType;
import com.mlorenzo.learnkafka.repository.LibraryEventRepository;
import com.mlorenzo.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

// Para lanzar las pruebas de integración sobre un Kafka embebido en memoria en vez del Kafka real
@EmbeddedKafka(topics = "library-events", partitions = 3)
// Sobrescribimos esta propiedad para indicar las localizaciones de los brokers de Kafka embebidos en memoria en
// vez de las localizaciones de los brokers reales
@TestPropertySource(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
@SpringBootTest
class LibraryEventConsumerIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @SpyBean
    LibraryEventService libraryEventService;

    Producer<Integer, String> kafkaProducer;

    @BeforeEach
    void setUp() {
        // Antes de ejecutar cada test, nos aseguramos de que los escuchadores de mensajes(Message Listeners) del
        // tienen consumidor tienen asignados las particiones del Topic.
        for(MessageListenerContainer messageListenerContainer: kafkaListenerEndpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        kafkaProducer = new DefaultKafkaProducerFactory<>(configs, new IntegerSerializer(), new StringSerializer())
                .createProducer();
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishSaveLibraryEventTest() throws InterruptedException, JsonProcessingException {
        //given
        String value = "{\"id\":null,\"book\":{\"id\":459,\"name\":\"Kafka with Spring Boot\","
                + "\"author\":\"John Doe\"},\"type\":\"NEW\"}";
        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord<>("library-events", null, value);
        CountDownLatch latch = new CountDownLatch(1);

        //when
        kafkaProducer.send(producerRecord);
        // Usamos un CountDownLatch porque el método "send" es asíncrono y se ejecuta en otro hilo
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService).save(isA(LibraryEvent.class));
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventRepository.findAll();
        assertEquals(1, libraryEventList.size());
        assertEquals(459, libraryEventList.get(0).getBook().getId());
    }

    @Test
    void publishUpdateLibraryEventTest() throws InterruptedException, JsonProcessingException {
        //given
        Book book = Book.builder()
                .id(459)
                .name("Kafka with Spring Boot")
                .author("John Doe")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .type(LibraryEventType.NEW)
                .build();
        LibraryEvent savedLibraryEvent = libraryEventRepository.save(libraryEvent);
        String value = "{\"id\":" + savedLibraryEvent.getId() + ",\"book\":{\"id\":459,"
                + "\"name\":\"Kafka with Spring Boot 2.x\",\"author\":\"John Doe\"},\"type\":\"UPDATE\"}";
        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord<>("library-events", savedLibraryEvent.getId(), value);
        CountDownLatch latch = new CountDownLatch(1);

        //when
        kafkaProducer.send(producerRecord);
        // Usamos un CountDownLatch porque el método "send" es asíncrono y se ejecuta en otro hilo
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService).update(isA(LibraryEvent.class));
        Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository.findById(libraryEvent.getId());
        assertTrue(optionalLibraryEvent.isPresent());
        assertEquals("Kafka with Spring Boot 2.x", optionalLibraryEvent.get().getBook().getName());
    }

    @Test
    void publishUpdateLibraryEventWithFailureAndNoRetriesTest() throws InterruptedException, JsonProcessingException {
        //given
        String value = "{\"id\":null,\"book\":{\"id\":459,\"name\":\"Kafka with Spring Boot 2.x\","
                + "\"author\":\"John Doe\"},\"type\":\"UPDATE\"}";
        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord<>("library-events", null, value);
        CountDownLatch latch = new CountDownLatch(1);

        //when
        kafkaProducer.send(producerRecord);
        // Usamos un CountDownLatch porque el método "send" es asíncrono y se ejecuta en otro hilo
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService, times(1)).update(isA(LibraryEvent.class));
    }

    @Test
    void publishUpdateLibraryEventWithFailureAndRetriesTest() throws InterruptedException, JsonProcessingException {
        //given
        // Nota: En nuestro caso, si se recibe un Library Event con id 999, se produce una excepción de tipo
        // RecoverableDataAccessException.
        String value = "{\"id\":999,\"book\":{\"id\":459,\"name\":\"Kafka with Spring Boot 2.x\","
                + "\"author\":\"John Doe\"},\"type\":\"UPDATE\"}";
        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord<>("library-events", null, value);
        CountDownLatch latch = new CountDownLatch(1);

        //when
        kafkaProducer.send(producerRecord);
        // Usamos un CountDownLatch porque el método "send" es asíncrono y se ejecuta en otro hilo
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumer, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService, times(3)).update(isA(LibraryEvent.class));
    }
}
