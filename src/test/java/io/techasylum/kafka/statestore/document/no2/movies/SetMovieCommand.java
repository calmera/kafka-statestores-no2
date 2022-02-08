package io.techasylum.kafka.statestore.document.no2.movies;

public record SetMovieCommand(String title, Integer year, Float rating) implements MovieEvent {
}
