package com.mlorenzo.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class LibraryEvent {
    private Integer id;

    @Valid
    @NotNull
    private Book book;

    private LibraryEventType type;
}
