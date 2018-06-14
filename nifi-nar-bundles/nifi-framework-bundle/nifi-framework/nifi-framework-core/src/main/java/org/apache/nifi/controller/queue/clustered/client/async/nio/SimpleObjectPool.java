package org.apache.nifi.controller.queue.clustered.client.async.nio;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class SimpleObjectPool<T> {
    private final int maxCount;
    private final BlockingQueue<T> queue;
    private final Supplier<T> factory;

    private int count;

    public SimpleObjectPool(final int maxCount, final Supplier<T> factory) {
        this.maxCount = maxCount;
        this.queue = new LinkedBlockingQueue<>(maxCount);
        this.factory = factory;
    }

    public synchronized T borrowObject() {
        final T existing = queue.poll();
        if (existing != null) {
            return existing;
        }

        if (count >= maxCount) {
            return null;
        }

        final T created = factory.get();
        count++;
        return created;
    }

    public synchronized void returnObject(T object) {
        queue.offer(object);
    }

    public synchronized void deactivate(T object) {
        if (queue.contains(object)) {
            throw new IllegalStateException("Cannot deactivate an object that is already in the queue.");
        }

        count--;
    }
}
