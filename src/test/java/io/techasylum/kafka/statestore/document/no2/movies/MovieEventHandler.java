package io.techasylum.kafka.statestore.document.no2.movies;

import io.techasylum.kafka.statestore.document.DocumentStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.dizitart.no2.objects.ObjectFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

public class MovieEventHandler implements Processor<String, MovieEvent, String, MovieCommandFeedback> {
    private final Logger LOGGER = LoggerFactory.getLogger(MovieEventHandler.class);

    private ProcessorContext<String, MovieCommandFeedback> context;
    private DocumentStore<String, Movie, ObjectFilter> store;

    @Override
    public void init(ProcessorContext<String, MovieCommandFeedback> context) {
        this.context = context;

        store = context.getStateStore("movies");
    }

    @Override
    public void process(Record<String, MovieEvent> record) {
        MovieCommandFeedback result = null;

        if (record.value() instanceof SetMovieCommand setCmd) {


            Movie fnd = this.store.get(record.key());
            if (fnd == null) {
                fnd = new Movie(
                        record.key(),
                        setCmd.title(),
                        setCmd.year(),
                        setCmd.rating());
            } else {
                fnd =  new Movie(
                        Optional.ofNullable(fnd.code()).orElse(record.key()),
                        Optional.ofNullable(fnd.title()).orElse(setCmd.title()),
                        Optional.ofNullable(fnd.year()).orElse(setCmd.year()),
                        Optional.ofNullable(fnd.rating()).orElse(setCmd.rating())
                );
            }

            this.store.put(record.key(), fnd);

            result = new MovieCommandFeedback(record.value(), null);

        } else if (record.value() instanceof DeleteMovieCommand delCmd) {
            Movie fnd = this.store.delete(record.key());
            if (fnd == null) {
                result = new MovieCommandFeedback(record.value(), "no such movie");
            } else {
                result = new MovieCommandFeedback(record.value(), null);
            }

        } else if (record.value() instanceof MovieCommandFeedback feedback) {
            if (feedback.command() instanceof SetMovieCommand) {
                if (feedback.hasError()) LOGGER.warn("unable to set movie " + record.key() + ": " + feedback.error());
                else LOGGER.info("movie " + record.key() + " set");
            } else if (feedback.command() instanceof DeleteMovieCommand) {
                if (feedback.hasError()) LOGGER.warn("unable to delete movie " + record.key() + ": " + feedback.error());
                else LOGGER.info("movie " + record.key() + " deleted");
            }

        } else {
            result = new MovieCommandFeedback(record.value(), "unknown event type");
        }

        if (result != null) {
            this.context.forward(new Record<>(record.key(), result, new Date().getTime()));
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
