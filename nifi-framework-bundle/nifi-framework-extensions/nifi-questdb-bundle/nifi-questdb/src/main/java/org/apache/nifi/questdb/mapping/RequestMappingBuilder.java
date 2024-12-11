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
package org.apache.nifi.questdb.mapping;

import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class RequestMappingBuilder<T> {
    private final Supplier<T> factory;
    private final List<Pair<Class<?>, BiConsumer<T, Object>>> fieldMappings = new LinkedList<>();

    private RequestMappingBuilder(final Supplier<T> factory) {
        this.factory = factory;
    }

    public <A> RequestMappingBuilder<T> addField(final Class<A> type, final BiConsumer<T, A> mapping) {
        fieldMappings.add(Pair.of(type, (BiConsumer<T, Object>) mapping));
        return this;
    }

    public RequestMappingBuilder<T> addStringField(final BiConsumer<T, String> mapping) {
        return addField(String.class, mapping);
    }

    public RequestMappingBuilder<T> addInstantField(final BiConsumer<T, Instant> mapping) {
        return addField(Instant.class, mapping);
    }

    public RequestMappingBuilder<T> addLongField(final BiConsumer<T, Long> mapping) {
        return addField(Long.class, mapping);
    }

    public RequestMappingBuilder<T> addIntegerField(final BiConsumer<T, Integer> mapping) {
        return addField(Integer.class, mapping);
    }

    public static <E> RequestMappingBuilder<E> of(final Supplier<E> factory) {
        return new RequestMappingBuilder<>(factory);
    }

    public RequestMapping<T> build() {
        if (fieldMappings.isEmpty()) {
            throw new IllegalArgumentException("There must be at least one declared field");
        }

        return new SimpleRequestMapping<>(factory, fieldMappings);
    }
}
