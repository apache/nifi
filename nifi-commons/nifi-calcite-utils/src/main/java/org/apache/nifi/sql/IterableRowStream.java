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

package org.apache.nifi.sql;

import java.util.Iterator;
import java.util.function.Function;

/**
 * A convenience implementation of RowStream that can be used to convert any {@link Iterable} to a RowStream.
 *
 * @param <T> the type object in the Iterable
 */
public class IterableRowStream<T> implements RowStream {
    private final Iterator<T> iterator;
    private final Function<T, Object[]> transform;

    /**
     * Creates an IterableRowStream that is capable of iterating over the objects in the given {@link Iterable}
     *
     * @param iterable  an Iterable that is capable of iterating over objects
     * @param transform a transformation function that is capable of converting the Objects in the Iterable to the <code>Object[]</code> necessary for Calcite.
     */
    public IterableRowStream(final Iterable<T> iterable, final Function<T, Object[]> transform) {
        this.iterator = iterable.iterator();
        this.transform = transform;
    }

    @Override
    public Object[] nextRow() {
        if (!iterator.hasNext()) {
            return null;
        }

        final T value = iterator.next();
        return transform.apply(value);
    }

    @Override
    public void close() {
    }
}
