# Kafka Streams Document Statestore

A custom state store based on [Nitrite](https://www.dizitart.org/nitrite-database.html) 
allowing for a mongodb-like api for querying data.

## Why
Working with Kafka Streams, you will have found yourself in situations where you
needed to retrieve data from a statestore. While there are different statestore 
implementations available, most are based on the ideas of a Key/Value store. We
can get pretty far by being smart in choosing our keys, but there are times you
would like to query the store based on something other than the key. 

With a KeyValueStore that would result in a full scan of the statestore, filtering out
the records that do not apply to the search criteria. For small statestores, this
is not an issue. For larger ones it becomes quite a performance hog.

Luckily, Kafka Streams allows custom statestore implementations so it is possible
to implement different underlying technologies for statestores. By choosing a 
document store as the base technology, we are able to push the querying down to
the underlying technology instead of having to deal with that ourselves.

## How
Use `DocumentStores.nitriteStore()` to create a new nitrite store instance and add it
to your topology. you will have to pass the store name, serdes for the key and value
as well as the name of the field you want to use as the key for the documents.

```java
Topology topology = new Topology()
    .addSource("sourceProcessor", Serdes.String().deserializer(), eventSerde.deserializer(), "movie-events")
    .addProcessor("commandHandler", MovieEventHandler::new, "sourceProcessor")
    topology.addStateStore(
        DocumentStores.nitriteStore("movies", Serdes.String(), movieSerde, Movie.class, "code"),
        "commandHandler")
    .addSink("sinkProcessor", "movie-events", Serdes.String().serializer(), eventSerde.serializer(), "commandHandler");
```

Within your processor, you can get the statestore from the `ProcessorContext` and
invoke the `find()` method to query the store. You can pass in any `ObjectFilter`
[provided by Nitrite](https://www.dizitart.org/nitrite-database/#filter)

```java
DocumentStore<String, Movie, ObjectFilter> store = context.getStateStore("movies");
QueryCursor<Movie> movies = store.find(and(ObjectFilters.regex("title", ".*Matrix.*")));
```

## Disclaimer
This project has not been through production grade testing yet. Use at your own risk!