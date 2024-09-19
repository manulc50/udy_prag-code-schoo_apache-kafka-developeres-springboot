package com.mlorenzo.learnkafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.stream.Collectors;

@Slf4j
@ControllerAdvice
public class LibraryEventControllerAdvice {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<String> handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        String errorMessages = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + " - " + fe.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining(", "));
        log.info("errorMessages: {}", errorMessages);
        return ResponseEntity.badRequest().body(errorMessages);
    }
}
