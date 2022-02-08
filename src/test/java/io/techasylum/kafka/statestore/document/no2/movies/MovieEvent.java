package io.techasylum.kafka.statestore.document.no2.movies;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SetMovieCommand.class, name = "set"),
        @JsonSubTypes.Type(value = DeleteMovieCommand.class, name = "delete"),
        @JsonSubTypes.Type(value = MovieCommandFeedback.class, name = "feedback")
})
public interface MovieEvent {

}
