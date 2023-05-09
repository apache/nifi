/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.security.util.crypto;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashAlgorithmTest {
    @Test
    void testDetermineBrokenAlgorithms() throws Exception {
        // Arrange
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());

        // Act
        final List<HashAlgorithm> brokenAlgorithms = algorithms.stream()
                .filter(algorithm -> !algorithm.isStrongAlgorithm())
                .collect(Collectors.toList());

        // Assert
        assertEquals(Arrays.asList(HashAlgorithm.MD2, HashAlgorithm.MD5, HashAlgorithm.SHA1), brokenAlgorithms);
    }

    @Test
    void testShouldBuildAllowableValueDescription() {
        // Arrange
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());

        // Act
        final List<String> descriptions = algorithms.stream()
                .map(algorithm -> algorithm.buildAllowableValueDescription())
                .collect(Collectors.toList());

        // Assert
        descriptions.forEach(description -> {
            final Pattern pattern = Pattern.compile(".* \\(\\d+ byte output\\).*");
            final Matcher matcher = pattern.matcher(description);
            assertTrue(matcher.find());
        });

        descriptions.stream()
                .filter(description -> {
                    final Pattern pattern = Pattern.compile("MD2|MD5|SHA-1");
                    final Matcher matcher = pattern.matcher(description);
                    return matcher.find();
                })
                .forEach(description -> assertTrue(description.contains("WARNING")));
    }

    @Test
    void testDetermineBlake2Algorithms() {
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());

        // Act
        final List<HashAlgorithm> blake2Algorithms = algorithms.stream()
                .filter(HashAlgorithm::isBlake2)
                .collect(Collectors.toList());

        // Assert
        assertEquals(Arrays.asList(HashAlgorithm.BLAKE2_160, HashAlgorithm.BLAKE2_256, HashAlgorithm.BLAKE2_384, HashAlgorithm.BLAKE2_512), blake2Algorithms);
    }

    @Test
    void testShouldMatchAlgorithmByName() {
        // Arrange
        final List<HashAlgorithm> algorithms = Arrays.asList(HashAlgorithm.values());

        // Act
        for (final HashAlgorithm algorithm : algorithms) {
            final List<String> transformedNames = Arrays.asList(algorithm.getName(), algorithm.getName().toUpperCase(), algorithm.getName().toLowerCase());

            for (final String name : transformedNames) {
                HashAlgorithm found = HashAlgorithm.fromName(name);

                // Assert
                assertEquals(name.toUpperCase(), found.getName());
            }
        }
    }
}
