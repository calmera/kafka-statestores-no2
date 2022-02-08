package io.techasylum.kafka.statestore.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Objects;

public class Document<K, V> {

    @JsonProperty("__key")
    private K key;

    @JsonUnwrapped
    private V value;

    public Document() {
    }

    public Document(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document<?, ?> document = (Document<?, ?>) o;
        return Objects.equals(key, document.key) && Objects.equals(value, document.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
