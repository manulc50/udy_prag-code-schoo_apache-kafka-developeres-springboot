package com.mlorenzo.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mlorenzo.learnkafka.domain.Book;
import com.mlorenzo.learnkafka.domain.LibraryEvent;
import com.mlorenzo.learnkafka.domain.LibraryEventType;
import com.mlorenzo.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventControllerTest {
    MockMvc mockMvc;
    ObjectMapper objectMapper;

    @Mock
    LibraryEventProducer libraryEventProducer;

    @InjectMocks
    LibraryEventController libraryEventController;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(libraryEventController)
                .setControllerAdvice(new LibraryEventControllerAdvice())
                .build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void postLibraryEventAsyncWithSendTest() throws Exception {
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

        when(libraryEventProducer.sendLibraryEventAsyncWithSend(isA(LibraryEvent.class))).thenReturn(null);

        //when //then
        mockMvc.perform(post("/v1/library-events/async/send")
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isAccepted());
    }

    @Test
    void postLibraryEventAsyncWithSend4xxTest() throws Exception {
        //given
        Book book = Book.builder()
                .id(null)
                .name("Kafka using Spring Boot")
                .author(null)
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .type(LibraryEventType.NEW)
                .build();
        String expectedErrorMessages = "book.author - must not be blank, book.id - must not be null";
        //when //then
        mockMvc.perform(post("/v1/library-events/async/send")
                        .content(objectMapper.writeValueAsString(libraryEvent))
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(expectedErrorMessages));
    }

    @Test
    void updateLibraryEventTest() throws Exception {
        //given
        Book book = Book.builder()
                .id(123)
                .author("John Doe")
                .name("Kafka Using Spring Boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(123)
                .book(book)
                .build();

        //when //then
        mockMvc.perform(put("/v1/library-events")
                        .content(objectMapper.writeValueAsString(libraryEvent))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isAccepted());
    }

    @Test
    void updateLibraryEvent4xxTest() throws Exception {
        //given
        Book book = Book.builder()
                .id(123)
                .author("John Doe")
                .name("Kafka Using Spring Boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .build();

        //when //then
        mockMvc.perform(put("/v1/library-events")
                        .content(objectMapper.writeValueAsString(libraryEvent))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please, pass the Library Event Id"));
    }
}
