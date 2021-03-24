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

/**
 * Wraps an object to be able to check equality conveniently, even if it doesn't have meaningful equals() implementation
 *  or it's not appropriate for a given context.
 * This is achieved by providing a list of transformer functions that usually extract properties of the object (and maybe transform them)
 *  and do equality checks on those.
 * <br/><br/>
 * Also provides a convenient toString() which can help identifying differences of expected and actual objects during unit tests.
 * <br/><br/>
 * Here's an example of a typical use-case:
 *
 * <pre>
 * {@code
 * @Test
 * public void testPersonEquals() throws Exception {
 *     // GIVEN
 *     Person expected = new Person();
 *     expected.setName("Joe");
 *     expected.setStuff(Arrays.asList(1, 2, 3));
 *
 *     Person actual = new Person();
 *     actual.setName("Joe");
 *     actual.setStuff(Arrays.asList(1, 2, 3));
 *
 *     // WHEN
 *     List<Function<Person, Object>> equalsProperties = Arrays.asList(
 *         Person::getName,
 *         Person::getStuff
 *     );
 *
 *     EqualsWrapper expectedWrapper = new EqualsWrapper(expected, equalsProperties);
 *     EqualsWrapper actualWrapper = new EqualsWrapper(actual, equalsProperties);
 *
 *     // THEN
 *     assertEquals(expectedWrapper, actualWrapper);
 * }
 *
 * private class Person {
 *     private String name;
 *     private List<Object> stuff = new ArrayList<>();
 *
 *     public String getName() {
 *         return name;
 *     }
 *     public void setName(String name) {
 *         this.name = name;
 *     }
 *
 *     public List<Object> getStuff() {
 *         return stuff;
 *     }
 *     public void setStuff(List<Object> stuff) {
 *         this.stuff = stuff;
 *     }
 * }
 * }
 * </pre>
 *
 * @param <T> The type of object to wrap
 */
public class EqualsWrapper<T> {
    private final T item;
    private final List<Function<T, Object>> propertyProviders;

    /**
     * Wraps an object and primes it for equality checks.
     *
     * @param item The item to be wrapped
     * @param propertyProviders List of functions with which to extract properties to use for equality checks
     */
    public EqualsWrapper(T item, List<Function<T, Object>> propertyProviders) {
        this.item = item;
        this.propertyProviders = propertyProviders;
    }

    /**
     * Wraps multiple objects and primes them for equality checks.
     *
     * @param items The items to be wrapped
     * @param propertyProviders List of functions with with to extract properties to use for equality checks
     * @param <T> The type of the objects to be wrapped
     * @return A list of wrapped objects
     */
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
