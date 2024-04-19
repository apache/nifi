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

import java.util.stream.Stream;

class RangeTest {

    @Test
    public void testNegativeLowerBoundary() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Range(-1, 3));
    }

    @Test
    public void testNegativeHigherBoundary() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Range(0, -1));
    }

    @Test
    public void testReorderedBoundaries() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new Range(7, 3));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("givenTestData")
    public void testCheckForOverlapping(final String name, final Range window, final int otherRangeLowerBoundary, final int otherRangeHigherBoundary, final Range.RelativePosition expectedResult) {
        Assertions.assertEquals(expectedResult, window.getOverlapping(otherRangeLowerBoundary, otherRangeHigherBoundary));
    }

    private static Stream<Arguments> givenTestData() {
        return Stream.of(
                Arguments.of("Fully within from start", new Range(0,5), 0, 2, Range.RelativePosition.FULLY_WITHIN_RANGE),
                Arguments.of("Head is within from start", new Range(0,5), 3, 7, Range.RelativePosition.HEAD_IS_WITHIN_RANGE),
                Arguments.of("After range from start", new Range(0,5), 8, 9, Range.RelativePosition.AFTER_RANGE),

                Arguments.of("Tail is within with lower boundary", new Range(2,7), 0, 2, Range.RelativePosition.TAIL_IS_WITHIN_RANGE),
                Arguments.of("Head is within with lower boundary", new Range(2,7), 3, 7, Range.RelativePosition.HEAD_IS_WITHIN_RANGE),
                Arguments.of("After range with lower boundary", new Range(2,7), 8, 9, Range.RelativePosition.AFTER_RANGE),

                Arguments.of("Before range with lower boundary", new Range(4,7), 0, 2, Range.RelativePosition.BEFORE_RANGE),
                Arguments.of("Middle is within with lower boundary", new Range(4,7), 3, 7, Range.RelativePosition.MIDDLE_IS_WITHIN_RANGE),

                Arguments.of("Fully within with a range of size 1", new Range(3,4), 3, 3, Range.RelativePosition.FULLY_WITHIN_RANGE),
                Arguments.of("Tail is within with a range of size 1", new Range(3,4), 2, 3, Range.RelativePosition.TAIL_IS_WITHIN_RANGE),
                Arguments.of("Head is within with a range of size 1", new Range(3,4), 3, 4, Range.RelativePosition.HEAD_IS_WITHIN_RANGE),

                // Where the window has no higher boundary
                Arguments.of("Fully within with no higher boundary, when start at lower boundary", new Range(3, 0), 3, 6, Range.RelativePosition.FULLY_WITHIN_RANGE),
                Arguments.of("Fully within with no higher boundary", new Range(3, 0), 11, 12, Range.RelativePosition.FULLY_WITHIN_RANGE),
                Arguments.of("Tail is within with no higher boundary", new Range(3, 0), 2, 4, Range.RelativePosition.TAIL_IS_WITHIN_RANGE)
        );
    }
}