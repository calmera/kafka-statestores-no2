package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.DocumentStore;
import io.techasylum.kafka.statestore.document.DocumentStores;
import io.techasylum.kafka.statestore.document.QueryCursor;
import io.techasylum.kafka.statestore.document.no2.movies.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.dizitart.no2.objects.ObjectFilter;
import org.dizitart.no2.objects.filters.ObjectFilters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static org.dizitart.no2.objects.filters.ObjectFilters.and;
import static org.dizitart.no2.objects.filters.ObjectFilters.gte;
import static org.junit.jupiter.api.Assertions.*;

class NitriteStoreTest {
    private TopologyTestDriver testDriver;
    private DocumentStore<String, Movie, ObjectFilter> store;

    private final Serde<Movie> movieSerde = new JsonSerde<>(Movie.class);
    private final Serde<MovieEvent> eventSerde = new JsonSerde<>(MovieEvent.class);

    private TestOutputTopic<String, MovieEvent> outputTopic;
    private TestInputTopic<String, MovieEvent> inputTopic;
    private TestOutputTopic<String, Movie> stateTopic;

    @BeforeEach
    public void setup() throws IOException {
        FileSystemUtils.deleteRecursively(Path.of("/tmp/kafka-streams/movieService"));

        Topology topology = new Topology()
                .addSource("sourceProcessor", Serdes.String().deserializer(), eventSerde.deserializer(), "movie-events")
                .addProcessor("commandHandler", MovieEventHandler::new, "sourceProcessor")
                .addStateStore(
                        DocumentStores.nitriteStore("movies", Serdes.String(), movieSerde, Movie.class, "code"),
                    "commandHandler")
                .addSink("sinkProcessor", "movie-events", Serdes.String().serializer(), eventSerde.serializer(), "commandHandler");

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "movieService");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        store = (DocumentStore<String, Movie, ObjectFilter>) testDriver.getStateStore("movies");

        outputTopic = testDriver.createOutputTopic("movie-events", Serdes.String().deserializer(), eventSerde.deserializer());
        inputTopic = testDriver.createInputTopic("movie-events", Serdes.String().serializer(), eventSerde.serializer());

        stateTopic = testDriver.createOutputTopic("movieService-movies-changelog", Serdes.String().deserializer(), movieSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldSetMovie() {
        String key = "the_matrix";
        MovieEvent cmd =  new SetMovieCommand("The Matrix", 1999, 8.7f);
        inputTopic.pipeInput(key, cmd);

        assertEquals(new KeyValue<>(key, new MovieCommandFeedback(cmd, null)), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());

        assertEquals(new KeyValue<>(key, new Movie(key, "The Matrix", 1999, 8.7f)), stateTopic.readKeyValue());
        assertTrue(stateTopic.isEmpty());

        assertEquals(new Movie("the_matrix", "The Matrix", 1999, 8.7f), store.get("the_matrix"));
    }

    @Test
    public void shouldRemoveMovie() {
        loadMovies();

        assertEquals(new Movie("the_matrix", "The Matrix", 1999, 8.7f), store.get("the_matrix"));

        inputTopic.pipeInput("the_matrix", new DeleteMovieCommand());

        assertNull(store.get("the_matrix"));
    }

    @Test
    public void shouldFindMovieWithRegexFilter() {
        loadMovies();

        QueryCursor<Movie> movies = store.find(and(ObjectFilters.regex("title", ".*Matrix.*")));
        assertEquals(4, movies.totalCount());
    }

    @Test
    public void shouldFindMovieWithMultipleFilter() {
        loadMovies();

        QueryCursor<Movie> movies = store.find(and(ObjectFilters.regex("title", ".*Matrix.*"), gte("year", 2003)));
        assertEquals(3, movies.totalCount());
    }

    private void loadMovies() {
        inputTopic.pipeInput("the_matrix", new SetMovieCommand("The Matrix", 1999, 8.7f));
        inputTopic.pipeInput("the_matrix_reloaded", new SetMovieCommand("The Matrix Reloaded", 2003, 7.2f));
        inputTopic.pipeInput("the_matrix_revolutions", new SetMovieCommand("The Matrix Revolutions", 2003, 6.8f));
        inputTopic.pipeInput("the_matrix_resurrections", new SetMovieCommand("The Matrix Resurrections", 2021, 6.0f));
        inputTopic.pipeInput("speed", new SetMovieCommand("Speed", 1994, 7.2f));
    }
}