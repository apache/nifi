/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe implementation of a RingBuffer
 *
 * @param <T>
 */
public class RingBuffer<T> {

    private final Object[] buffer;
    private int insertionPointer = 0;
    private boolean filled = false;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public RingBuffer(final int size) {
        buffer = new Object[size];
    }

    /**
     * Adds the given value to the RingBuffer and returns the value that was
     * removed in order to make room.
     *
     * @param value
     * @return
     */
    @SuppressWarnings("unchecked")
    public T add(final T value) {
        Objects.requireNonNull(value);

        writeLock.lock();
        try {
            final Object removed = buffer[insertionPointer];

            buffer[insertionPointer] = value;

            if (insertionPointer == buffer.length - 1) {
                filled = true;
            }

            insertionPointer = (insertionPointer + 1) % buffer.length;
            return (T) removed;
        } finally {
            writeLock.unlock();
        }
    }

    public int getSize() {
        readLock.lock();
        try {
            return filled ? buffer.length : insertionPointer;
        } finally {
            readLock.unlock();
        }
    }

    public List<T> getSelectedElements(final Filter<T> filter) {
        return getSelectedElements(filter, Integer.MAX_VALUE);
    }

    public List<T> getSelectedElements(final Filter<T> filter, final int maxElements) {
        final List<T> selected = new ArrayList<>(1000);
        int numSelected = 0;
        readLock.lock();
        try {
            for (int i = 0; i < buffer.length && numSelected < maxElements; i++) {
                final int idx = (insertionPointer + i) % buffer.length;
                final Object val = buffer[idx];
                if (val == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                final T element = (T) val;
                if (filter.select(element)) {
                    selected.add(element);
                    numSelected++;
                }
            }
        } finally {
            readLock.unlock();
        }
        return selected;
    }

    public int countSelectedElements(final Filter<T> filter) {
        int numSelected = 0;
        readLock.lock();
        try {
            for (int i = 0; i < buffer.length; i++) {
                final int idx = (insertionPointer + i) % buffer.length;
                final Object val = buffer[idx];
                if (val == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                final T element = (T) val;
                if (filter.select(element)) {
                    numSelected++;
                }
            }
        } finally {
            readLock.unlock();
        }

        return numSelected;
    }

    /**
     * Removes all elements from the RingBuffer that match the given filter
     *
     * @param filter
     * @return
     */
    public int removeSelectedElements(final Filter<T> filter) {
        int count = 0;

        writeLock.lock();
        try {
            for (int i = 0; i < buffer.length; i++) {
                final int idx = (insertionPointer + i + 1) % buffer.length;
                final Object val = buffer[idx];
                if (val == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                final T element = (T) val;

                if (filter.select(element)) {
                    buffer[idx] = null;
                }
            }
        } finally {
            writeLock.unlock();
        }

        return count;
    }

    public List<T> asList() {
        return getSelectedElements(new Filter<T>() {
            @Override
            public boolean select(final T value) {
                return true;
            }
        });
    }

    public T getOldestElement() {
        readLock.lock();
        try {
            return getElementData(insertionPointer);
        } finally {
            readLock.unlock();
        }
    }

    public T getNewestElement() {
        readLock.lock();
        try {
            int index = (insertionPointer == 0) ? buffer.length - 1 : insertionPointer - 1;
            return getElementData(index);
        } finally {
            readLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private T getElementData(final int index) {
        readLock.lock();
        try {
            return (T) buffer[index];
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Iterates over each element in the RingBuffer, calling the
     * {@link ForEachEvaluator#evaluate(Object) evaluate} method on each element
     * in the RingBuffer. If the Evaluator returns {@code false}, the method
     * will skip all remaining elements in the RingBuffer; otherwise, the next
     * element will be evaluated until all elements have been evaluated.
     *
     * @param evaluator
     */
    public void forEach(final ForEachEvaluator<T> evaluator) {
        forEach(evaluator, IterationDirection.FORWARD);
    }

    /**
     * Iterates over each element in the RingBuffer, calling the
     * {@link ForEachEvaluator#evaluate(Object) evaluate} method on each element
     * in the RingBuffer. If the Evaluator returns {@code false}, the method
     * will skip all remaining elements in the RingBuffer; otherwise, the next
     * element will be evaluated until all elements have been evaluated.
     *
     * @param evaluator
     * @param iterationDirection the order in which to iterate over the elements
     * in the RingBuffer
     */
    public void forEach(final ForEachEvaluator<T> evaluator, final IterationDirection iterationDirection) {
        readLock.lock();
        try {
            final int startIndex;
            final int endIndex;
            final int increment;

            if (iterationDirection == IterationDirection.FORWARD) {
                startIndex = 0;
                endIndex = buffer.length - 1;
                increment = 1;
            } else {
                startIndex = buffer.length - 1;
                endIndex = 0;
                increment = -1;
            }

            for (int i = startIndex; (iterationDirection == IterationDirection.FORWARD ? i <= endIndex : i >= endIndex); i += increment) {
                final int idx = (insertionPointer + i) % buffer.length;
                final Object val = buffer[idx];
                if (val == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                final T element = (T) val;
                if (!evaluator.evaluate(element)) {
                    return;
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public static interface Filter<S> {

        boolean select(S value);
    }

    /**
     * Defines an interface that can be used to iterate over all of the elements
     * in the RingBuffer via the {@link #forEach} method
     *
     * @param <S>
     */
    public static interface ForEachEvaluator<S> {

        /**
         * Evaluates the given element and returns {@code true} if the next
         * element should be evaluated, {@code false} otherwise
         *
         * @param value
         * @return
         */
        boolean evaluate(S value);
    }

    public static enum IterationDirection {

        FORWARD,
        BACKWARD;
    }
}
