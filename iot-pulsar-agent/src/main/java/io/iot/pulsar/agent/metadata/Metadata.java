package io.iot.pulsar.agent.metadata;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;


public interface Metadata<K, V> {

    /**
     * Return the current value.
     *
     * @param key Metadata key
     * @return Current value
     */
    @Nonnull
    CompletableFuture<Optional<V>> get(@Nonnull K key);

    /**
     * Put the new value.
     *
     * @param key   Key
     * @param value Value
     * @return previous value
     */
    @Nonnull
    CompletableFuture<Optional<V>> put(@Nonnull K key, @Nonnull V value);

    /**
     * Delete the value by key.
     *
     * @param key key
     * @return previous value
     */
    @Nonnull
    CompletableFuture<Optional<V>> delete(@Nonnull K key);
}
