package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.QueryCursor;
import org.dizitart.no2.objects.Cursor;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class NitriteQueryCursorWrapper<V> implements QueryCursor<V> {
    private final Cursor<V> cursor;

    public NitriteQueryCursorWrapper(Cursor<V> cursor) {
        this.cursor = cursor;
    }

    @NotNull
    @Override
    public Iterator<V> iterator() {
        return this.cursor.iterator();
    }

    @Override
    public void forEach(Consumer<? super V> action) {
        this.cursor.forEach(action);
    }

    @Override
    public Spliterator<V> spliterator() {
        return this.cursor.spliterator();
    }

    @Override
    public boolean hasMore() {
        return this.cursor.hasMore();
    }

    @Override
    public int size() {
        return this.cursor.size();
    }

    @Override
    public int totalCount() {
        return this.cursor.totalCount();
    }

    @Override
    public V firstOrDefault() {
        return this.cursor.firstOrDefault();
    }

    @Override
    public List<V> toList() {
        return this.cursor.toList();
    }
}
