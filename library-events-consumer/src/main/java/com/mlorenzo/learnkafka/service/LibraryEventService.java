package com.mlorenzo.learnkafka.service;

import com.mlorenzo.learnkafka.entity.LibraryEvent;

public interface LibraryEventService {
    void save(LibraryEvent libraryEvent);
    void update(LibraryEvent libraryEvent);
}
