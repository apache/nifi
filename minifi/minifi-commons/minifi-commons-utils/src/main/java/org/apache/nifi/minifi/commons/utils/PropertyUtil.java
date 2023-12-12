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
package org.apache.nifi.minifi.commons.utils;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class PropertyUtil {
    private static final String DOT = ".";
    private static final String UNDERSCORE = "_";
    private static final String HYPHEN = "-";

    private PropertyUtil() {

    }

    public static Optional<String> resolvePropertyValue(String name, Map<?, ?> properties) {
        return Optional.of(name)
            .flatMap(n -> getPropertyValue(n, properties))
            .or(() -> Optional.of(name.toUpperCase())
                .filter(uppercasedName -> !name.equals(uppercasedName))
                .flatMap(n -> getPropertyValue(n, properties)));
    }

    private static Optional<String> getPropertyValue(String name, Map<?, ?> properties) {
        return keyPermutations(name)
            .filter(properties::containsKey)
            .findFirst()
            .map(properties::get)
            .map(String::valueOf);
    }

    private static Stream<String> keyPermutations(String name) {
        return Stream.of(name, name.replace(DOT, UNDERSCORE), name.replace(HYPHEN, UNDERSCORE), name.replace(DOT, UNDERSCORE).replace(HYPHEN, UNDERSCORE)).distinct();
    }

}
