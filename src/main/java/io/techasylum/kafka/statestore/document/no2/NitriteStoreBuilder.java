package io.techasylum.kafka.statestore.document.no2;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;

public class NitriteStoreBuilder<K, V> implements StoreBuilder<NitriteStore<K, V>> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Class<V> valueClass;
    private final String keyFieldName;

    private Map<String, String> logConfig;

    public NitriteStoreBuilder(String name, Serde<K> keySerde, Serde<V> valueSerde, Class<V> valueClass, String keyFieldName) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.valueClass = valueClass;
        this.keyFieldName = keyFieldName;
    }

    @Override
    public StoreBuilder<NitriteStore<K, V>> withCachingEnabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public StoreBuilder<NitriteStore<K, V>> withCachingDisabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public StoreBuilder<NitriteStore<K, V>> withLoggingEnabled(Map<String, String> config) {
        this.logConfig = config;
        return this;
    }

    @Override
    public StoreBuilder<NitriteStore<K, V>> withLoggingDisabled() {
        throw new UnsupportedOperationException("logging cannot be turned off for nitrite stores");
    }

    @Override
    public NitriteStore<K, V> build() {
        return new NitriteStore<>(this.name, this.keySerde, this.valueSerde, this.valueClass, this.keyFieldName);
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return true;
    }

    @Override
    public String name() {
        return this.name;
    }
}
