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
package org.apache.nifi.processors.cipher.age;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Shared key reader abstraction for age-encryption.org public and private keys using configurable prefix and pattern
 *
 * @param <T> Type of parsed key
 */
abstract class AbstractAgeKeyReader<T> implements AgeKeyReader<T> {

    private final AgeKeyIndicator ageKeyIndicator;

    /**
     * Key Reader with prefix for initial line filtering and pattern for subsequent matching
     *
     * @param ageKeyIndicator Key Indicator
     */
    AbstractAgeKeyReader(final AgeKeyIndicator ageKeyIndicator) {
        this.ageKeyIndicator = ageKeyIndicator;
    }

    /**
     * Read keys from Input Stream based on lines starting with indicated prefix and matched against configured pattern
     *
     * @param inputStream Input Stream to be parsed
     * @return Parsed key objects
     * @throws IOException Thrown on failures reading Input Stream
     */
    @Override
    public List<T> read(final InputStream inputStream) throws IOException {
        Objects.requireNonNull(inputStream, "Input Stream required");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final Set<String> keys = reader.lines()
                    .filter(line -> line.startsWith(ageKeyIndicator.getPrefix()))
                    .map(ageKeyIndicator.getPattern()::matcher)
                    .filter(Matcher::matches)
                    .map(Matcher::group)
                    .collect(Collectors.toSet());

            return readKeys(keys);
        }
    }

    /**
     * Read encoded keys filtered from Input Stream based on matched pattern
     *
     * @param keys Encoded keys
     * @return Parsed key objects
     * @throws IOException Thrown on failures reading encoded keys
     */
    protected abstract List<T> readKeys(Set<String> keys) throws IOException;
}
