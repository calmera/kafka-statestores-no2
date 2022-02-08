package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.NitriteStore;
import io.techasylum.kafka.statestore.document.no2.NitriteStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;

public final class DocumentStores {
    public static <K, V> StoreBuilder<NitriteStore<K, V>> nitriteStore(String name, Serde<K> keySerde, Serde<V> valueSerde, Class<V> valueClass, String keyFieldName) {
        return new NitriteStoreBuilder<>(name, keySerde, valueSerde, valueClass, keyFieldName);
    }
}
