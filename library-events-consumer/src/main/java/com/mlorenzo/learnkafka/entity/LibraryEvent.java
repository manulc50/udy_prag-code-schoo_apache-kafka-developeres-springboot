package com.mlorenzo.learnkafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@Entity
@Table(name = "library_events")
public class LibraryEvent {

    @Id
    @GeneratedValue
    private Integer id;

    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
    private Book book;

    @Enumerated(EnumType.STRING)
    private LibraryEventType type;
}
