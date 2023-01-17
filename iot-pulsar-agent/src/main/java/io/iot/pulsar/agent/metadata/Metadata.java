package io.iot.pulsar.agent.metadata;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public interface Metadata<K, V> {

    /**
     * Return the current value.
     *
     * @param key Metadata key
     */
    @Nonnull
    CompletableFuture<Optional<V>> get(@Nonnull K key);

    /**
     * Put the new value.
     *
     * @param key   Key
     * @param value Value
     */
    @Nonnull
    CompletableFuture<Void> put(@Nonnull K key, @Nullable V value);

    /**
     * Delete the value by key.
     *
     * @param key key
     */
    @Nonnull
    CompletableFuture<Void> delete(@Nonnull K key);

    @Nonnull
    CompletableFuture<Void> listen(@Nonnull K key, @Nonnull Consumer<V> listener);

    @Nonnull
    CompletableFuture<Void> close();
}
