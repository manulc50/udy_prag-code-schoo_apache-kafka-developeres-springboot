package com.mlorenzo.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mlorenzo.learnkafka.domain.LibraryEvent;
import com.mlorenzo.learnkafka.domain.LibraryEventType;
import com.mlorenzo.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/v1/library-events")
public class LibraryEventController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/async")
    public ResponseEntity<Void> postLibraryEventAsync(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException {
        libraryEvent.setType(LibraryEventType.NEW);
        log.info("Before sendLibraryEvent");
        libraryEventProducer.sendLibraryEventAsync(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/async/send")
    public ResponseEntity<Void> postLibraryEventAsyncWithSend(@Valid @RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException {
        libraryEvent.setType(LibraryEventType.NEW);
        log.info("Before sendLibraryEvent");
        libraryEventProducer.sendLibraryEventAsyncWithSend(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/sync")
    public ResponseEntity<Void> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException {
        libraryEvent.setType(LibraryEventType.NEW);
        log.info("Before sendLibraryEvent");
        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSync(libraryEvent);
        log.info("SendResult: {}", sendResult.toString());
        log.info("After sendLibraryEvent");
        return ResponseEntity.accepted().build();
    }

    @PutMapping
    public ResponseEntity<?> putLibraryEvent(@Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        if(libraryEvent.getId() == null)
            return new ResponseEntity<>("Please, pass the Library Event Id", HttpStatus.BAD_REQUEST);
        libraryEvent.setType(LibraryEventType.UPDATE);
        log.info("Before sendLibraryEvent");
        libraryEventProducer.sendLibraryEventAsyncWithSend(libraryEvent);
        log.info("After sendLibraryEvent");
        return new ResponseEntity<>(HttpStatus.ACCEPTED);
    }
}
