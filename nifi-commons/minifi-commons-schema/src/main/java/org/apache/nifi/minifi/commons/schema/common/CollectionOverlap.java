/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectionOverlap<T> {
    private final Set<T> elements;
    private final Set<T> duplicates;

    public CollectionOverlap(Collection<T>... collections) {
        this(Arrays.stream(collections).map(c -> c.stream()));
    }

    public CollectionOverlap(Stream<T>... streams) {
        this(Arrays.stream(streams).map(Function.identity()));
    }

    public CollectionOverlap(Stream<Stream<T>> streams) {
        Set<T> elements = new HashSet<>();
        this.duplicates = Collections.unmodifiableSet(streams.flatMap(Function.identity()).sequential().filter(s -> !elements.add(s)).collect(Collectors.toSet()));
        this.elements = Collections.unmodifiableSet(elements);
    }

    public Set<T> getElements() {
        return elements;
    }

    public Set<T> getDuplicates() {
        return duplicates;
    }
}
