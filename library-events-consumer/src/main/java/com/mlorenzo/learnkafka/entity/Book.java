package com.mlorenzo.learnkafka.entity;

import lombok.*;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@Entity
@Table(name = "books")
public class Book {

    @Id
    private Integer id;

    private String name;
    private String author;

    @ToString.Exclude
    @OneToOne
    private LibraryEvent libraryEvent;
}
