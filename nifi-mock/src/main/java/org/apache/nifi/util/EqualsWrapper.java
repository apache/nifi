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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EqualsWrapper<T> {
    private final T item;
    private final List<Function<T, Object>> propertyProviders;

    public EqualsWrapper(T item, List<Function<T, Object>> propertyProviders) {
        this.item = item;
        this.propertyProviders = propertyProviders;
    }

    public static <T> List<EqualsWrapper<T>> wrapList(Collection<T> items, List<Function<T, Object>> propertyProviders) {
        List wrappers = items.stream().map(item -> new EqualsWrapper(item, propertyProviders)).collect(Collectors.toList());

        return wrappers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null
            || getClass() != o.getClass()
            || !Arrays.equals(o.getClass().getGenericInterfaces(), o.getClass().getGenericInterfaces())
        ) {
            return false;
        }

        final EqualsWrapper<T> that = (EqualsWrapper<T>) o;
        final EqualsBuilder equalsBuilder = new EqualsBuilder();

        for (Function<T, Object> propertyProvider : propertyProviders) {
            equalsBuilder.append(propertyProvider.apply(item), propertyProvider.apply(that.item));
        }

        return equalsBuilder.isEquals();
    }

    @Override
    public int hashCode() {
        final HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(17, 37);

        for (Function<T, Object> propertyProvider : propertyProviders) {
            hashCodeBuilder.append(propertyProvider.apply(item));
        }

        return hashCodeBuilder.toHashCode();
    }

    @Override
    public String toString() {
        final StringJoiner stringJoiner = new StringJoiner(",\n\t", "{\n\t", "\n}");

        for (Function<T, Object> propertySupplier : propertyProviders) {
            stringJoiner.add(Optional.ofNullable(propertySupplier.apply(item)).orElse("N/A").toString());
        }

        return stringJoiner.toString();
    }
}
