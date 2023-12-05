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
package org.apache.nifi.questdb;

import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public final class InsertMappingBuilder<T> {
    private final Class<T> type;
    private final List<Pair<Class<?>, Function<T, ?>>> fieldMappings = new LinkedList<>();
    private int timestampPosition = -1;

    private InsertMappingBuilder(final Class<T> type) {
        this.type = type;
    }

    public InsertMappingBuilder<T> timestampAt(final int timestampPosition) {
        this.timestampPosition = timestampPosition;
        return this;
    }

    public <A> InsertMappingBuilder<T> addField(final Class<A> type, final Function<T, A> mapping) {
        fieldMappings.add(Pair.of(type, mapping));
        return this;
    }

    public InsertMappingBuilder<T> addLongField(final Function<T, Long> mapping) {
        return addField(Long.class, mapping);
    }

    public InsertMappingBuilder<T> addIntegerField(final Function<T, Integer> mapping) {
        return addField(Integer.class, mapping);
    }

    public InsertMappingBuilder<T> addInstantField(final Function<T, Instant> mapping) {
        return addField(Instant.class, mapping);
    }

    public InsertMappingBuilder<T> addStringField(final Function<T, String> mapping) {
        return addField(String.class, mapping);
    }

    public InsertMapping<T> build() {
        if (fieldMappings.isEmpty()) {
            throw new IllegalArgumentException("There must be at least one declared field");
        }

        if (timestampPosition < 0 || timestampPosition > fieldMappings.size() -1) {
            throw new IllegalArgumentException(String.format("The timestamp field number must withing the range 0 and %d (determined by the number of fields)", fieldMappings.size() -1));
        }

        // TODO
        if (!fieldMappings.get(timestampPosition).getLeft().equals(Instant.class)) {
            throw new IllegalArgumentException("The timestamp field must be an Instant");
        }

        return new SimpleInsertMapping<>(type, timestampPosition, fieldMappings);
    }

    public static <E> InsertMappingBuilder<E> of(final Class<E> type) {
        return new InsertMappingBuilder<>(type);
    }
}
