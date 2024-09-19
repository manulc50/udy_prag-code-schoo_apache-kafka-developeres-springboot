package com.mlorenzo.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mlorenzo.learnkafka.entity.LibraryEvent;
import com.mlorenzo.learnkafka.entity.LibraryEventType;
import com.mlorenzo.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsConsumer {

    @Autowired
    private LibraryEventService libraryEventService;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord: {}", consumerRecord);
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        switch(libraryEvent.getType()) {
            case NEW: {
                libraryEventService.save(libraryEvent);
                break;
            }
            case UPDATE: {
                libraryEventService.update(libraryEvent);
                break;
            }
            default: {
                log.info("Invalid Library Event Type");
                break;
            }
        }
    }
}
