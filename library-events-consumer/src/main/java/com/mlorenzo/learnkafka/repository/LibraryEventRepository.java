package com.mlorenzo.learnkafka.repository;

import com.mlorenzo.learnkafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {
}
