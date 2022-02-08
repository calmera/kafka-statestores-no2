package io.techasylum.kafka.statestore.document.no2.movies;

public record MovieCommandFeedback(MovieEvent command, String error) implements MovieEvent {
    public boolean hasError() {
        return this.error != null;
    }
}
