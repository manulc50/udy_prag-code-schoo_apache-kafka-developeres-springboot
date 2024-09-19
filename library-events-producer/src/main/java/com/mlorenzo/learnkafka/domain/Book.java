package com.mlorenzo.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class Book {

    @NotNull
    private Integer id;

    @NotBlank
    private String name;

    @NotBlank
    private String author;
}
