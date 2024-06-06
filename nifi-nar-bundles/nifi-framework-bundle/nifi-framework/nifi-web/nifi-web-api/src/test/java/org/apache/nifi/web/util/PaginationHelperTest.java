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
package org.apache.nifi.web.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class PaginationHelperTest {

    @Test
    public void testCreatingWithNegativeOffset() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaginationHelper.paginateByContainedItems(getTestInput(), -1, 3, Function.identity(), (original, partialList) -> partialList));
    }

    @Test
    public void testCreatingWithNegativeLimit() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> PaginationHelper.paginateByContainedItems(getTestInput(), 0, -1, Function.identity(), (original, partialList) -> partialList));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("givenTestData")
    public void testPaginateByContainedItems(final String name, final int offset, final int limit, final List<Integer> expectedResult) {
        final List<List<Integer>> result = PaginationHelper.paginateByContainedItems(getTestInput(), offset, limit, Function.identity(), (original, partialList) -> partialList);
        final List<Integer> flatten = result.stream().flatMap(Collection::stream).collect(Collectors.toList());
        Assertions.assertIterableEquals(expectedResult, flatten);
    }

    private static Stream<Arguments> givenTestData() {
        return Stream.of(
                Arguments.of("Full result set", 0, 0, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
                Arguments.of("Offset only when starts with full", 3, 0, Arrays.asList(4, 5, 6, 7, 8, 9, 10, 11, 12)),
                Arguments.of("Offset only when starts with partial", 4, 0, Arrays.asList(5, 6, 7, 8, 9, 10, 11, 12)),
                Arguments.of("From beginning with partial", 0, 5, Arrays.asList(1, 2, 3, 4, 5)),
                Arguments.of("Beginning with partial and offset", 1, 5, Arrays.asList(2, 3, 4, 5, 6)),
                Arguments.of("Middle partial only", 4, 2, Arrays.asList(5, 6)),
                Arguments.of("From the end with partial", 9, 3, Arrays.asList(10, 11, 12)),
                Arguments.of("From the end with partial when spills over", 9, 5, Arrays.asList(10, 11, 12)),
                Arguments.of("Clear cut", 3, 5, Arrays.asList(4, 5, 6, 7, 8)),
                Arguments.of("Long result", 2, 8, Arrays.asList(3, 4, 5, 6, 7, 8, 9, 10)),
                Arguments.of("All the original items", 0, 12, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
                Arguments.of("Second partial", 5, 7, Arrays.asList(6, 7, 8, 9, 10, 11, 12)),
                Arguments.of("From the end of the range", 12, 3, Collections.emptyList()),
                Arguments.of("Outside of the range", 14, 4, Collections.emptyList())
        );
    }

    private static List<List<Integer>> getTestInput() {
        final List<Integer> l1 = Arrays.asList(1, 2, 3);
        final List<Integer> l2 = Arrays.asList(4, 5, 6, 7, 8);
        final List<Integer> l3 = Arrays.asList(9, 10, 11, 12);
        return new ArrayList<>(Arrays.asList(l1, l2, l3));
    }
}