package io.techasylum.kafka.statestore.document;

import org.apache.kafka.streams.errors.InvalidStateStoreException;

public interface ReadonlyDocumentStore<K, V, Q> {
    QueryCursor<V> find(Q query);


    /**
     * Get the value corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    V get(K key);

}
