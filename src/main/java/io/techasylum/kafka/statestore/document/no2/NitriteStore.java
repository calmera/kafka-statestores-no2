package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.DocumentStore;
import io.techasylum.kafka.statestore.document.QueryCursor;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.StateSerdes;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteBuilder;
import org.dizitart.no2.exceptions.NitriteException;
import org.dizitart.no2.objects.Cursor;
import org.dizitart.no2.objects.ObjectFilter;
import org.dizitart.no2.objects.ObjectRepository;
import org.dizitart.no2.objects.filters.ObjectFilters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerde;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareValueSerde;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

public class NitriteStore<K, V> implements DocumentStore<K, V, ObjectFilter> {
    final String name;
    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    final String keyFieldName;

    Class<V> valueClass;
    StateSerdes<K, V> serdes;

    private Nitrite db;
    private ObjectRepository<V> repo;

    InternalProcessorContext context;

    public NitriteStore(String name, Serde<K> keySerde, Serde<V> valueSerde, Class<V> valueClass, String keyFieldName) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyFieldName = keyFieldName;
        this.valueClass = valueClass;
    }

// == Store Properties ================================================================================================

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return !this.db.isClosed();
    }

// == Store Level Administration ======================================================================================


    @Override
    public void init(ProcessorContext context, StateStore root) {
        throw new NotImplementedException("deprecated.");
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = asInternalProcessorContext(context);

        initStoreSerde(context);
        openDB(context.appConfigs(), context.stateDir());

        context.register(root, new NitriteStore<K, V>.NitriteRestoreCallback(this));
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        serdes = new StateSerdes<>(
                changelogTopic != null ?
                        changelogTopic :
                        ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName),
                prepareKeySerde(keySerde, new SerdeGetter(context)),
                prepareValueSerde(valueSerde, new SerdeGetter(context))
        );
    }

    @Override
    public void flush() {
        if (this.db == null) return;
        this.db.commit();
    }

    @Override
    public void close() {
        if (this.db == null) return;
        this.db.close();
    }

    private void validateStoreOpen() {
        if (!isOpen()) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    void openDB(final Map<String, Object> configs, final File stateDir) {
        File dbDir = new File(stateDir, name);

        NitriteBuilder builder = Nitrite.builder()
                .filePath(dbDir);

        final Class<NitriteConfigSetter> configSetterClass =
                (Class<NitriteConfigSetter>) configs.get(NitriteConfig.NITRITE_CONFIG_SETTER_CLASS_CONFIG);
        if (configSetterClass != null) {
            NitriteConfigSetter configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, builder, configs);
        }

        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
//            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        try {
            this.db = builder.openOrCreate();
            this.repo = this.db.getRepository(this.valueClass);
        } catch (NitriteException ne) {
            throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir, ne);
        }
    }

// == Operations ======================================================================================================

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        Cursor<V> items = this.repo.find(ObjectFilters.eq(keyFieldName, key));

        int cnt = items.totalCount();
        if (cnt == 0) {
            return null;
        } else if (cnt == 1) {
            return items.firstOrDefault();
        } else {
            throw new ProcessorStateException(String.format("Multiple results for key %s!", key));
        }
    }

    @Override
    public QueryCursor<V> find(ObjectFilter query) {
        Objects.requireNonNull(query, "query cannot be null");
        validateStoreOpen();

        Cursor<V> c = this.repo.find(query);

        return new NitriteQueryCursorWrapper<>(c);
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        validateStoreOpen();

        this.store(key, value);
        this.log(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        validateStoreOpen();

        final V previous = this.storeIfAbsent(key, value);
        if (previous == null) {
            // then it was absent
            log(key, value);
        }
        return previous;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        Objects.requireNonNull(entries, "entries cannot be null");
        validateStoreOpen();

        this.storeAll(entries);
        for (KeyValue<K, V> entry : entries) {
            this.log(entry.key, entry.value);
        }
    }

    @Override
    public V delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        final V oldValue = this.remove(key);
        log(key, null);
        return oldValue;
    }

    void log(final K key,
             final V value) {
        context.logChange(
                name(),
                Bytes.wrap(this.serdes.rawKey(key)),
                this.serdes.rawValue(value),
                context.timestamp());
    }

// == Internal Operations (no logging) ================================================================================

    protected synchronized void store(K key, V value) {
        this.repo.update(ObjectFilters.eq(keyFieldName, key), value, true);
    }

    protected synchronized V storeIfAbsent(K key, V value) {
        V existing = this.get(key);
        if (existing == null) {
            this.store(key, value);
        }

        return existing;
    }

    protected synchronized void storeAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries) {
            this.repo.update(ObjectFilters.eq(keyFieldName, entry.key), entry.value, true);
        }
    }

    protected V remove(K key) {
        V result = this.get(key);
        if (result == null) {
            return null;
        }

        this.repo.remove(ObjectFilters.eq(keyFieldName, key));

        return result;
    }

// == Replay ==========================================================================================================

    public class NitriteRestoreCallback implements StateRestoreCallback {
        private final NitriteStore<K, V> store;

        public NitriteRestoreCallback(NitriteStore<K, V> store) {
            this.store = store;
        }

        @Override
        public void restore(byte[] key, byte[] value) {
            K k = NitriteStore.this.serdes.keyFrom(key);
            V v = NitriteStore.this.serdes.valueFrom(value);

            this.store.store(k, v);
        }
    }
}
