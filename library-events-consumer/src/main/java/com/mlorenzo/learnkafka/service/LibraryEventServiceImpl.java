package com.mlorenzo.learnkafka.service;

import com.mlorenzo.learnkafka.entity.LibraryEvent;
import com.mlorenzo.learnkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class LibraryEventServiceImpl implements LibraryEventService {

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Override
    public void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        LibraryEvent savedLibraryEvent = libraryEventRepository.save(libraryEvent);
        log.info("Successfully persisted the Library Event: {}", savedLibraryEvent);
    }

    @Override
    public void update(LibraryEvent libraryEvent) {
        if(libraryEvent.getId() == null)
            throw new IllegalArgumentException("Library Event Id is missing");
        if(libraryEvent.getId() == 999)
            throw new RecoverableDataAccessException("Temporary Network Issue");
        Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository.findById(libraryEvent.getId());
        if(optionalLibraryEvent.isEmpty())
            throw new IllegalStateException("Library Event not found");
        save(libraryEvent);
    }
}
