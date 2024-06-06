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

class ClosedOpenIntervalTest {

    @Test
    public void testNegativeLowerBoundary() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClosedOpenInterval(-1, 3));
    }

    @Test
    public void testNegativeHigherBoundary() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClosedOpenInterval(0, -1));
    }

    @Test
    public void testSwitchedBoundaries() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClosedOpenInterval(7, 3));
    }

    @Test
    public void testCompareWhenOtherLowerBoundaryIsNegative() {
        final ClosedOpenInterval testSubject = new ClosedOpenInterval(1, 3);
        Assertions.assertThrows(IllegalArgumentException.class, () -> testSubject.getRelativePositionOf(-1, 4));
    }

    @Test
    public void testCompareWhenOtherBoundariesAreSwitched() {
        final ClosedOpenInterval testSubject = new ClosedOpenInterval(1, 3);
        Assertions.assertThrows(IllegalArgumentException.class, () -> testSubject.getRelativePositionOf(9, 4));
    }

    @Test
    public void testCompareWhenOtherHigherBoundaryIsUnspecified() {
        final ClosedOpenInterval testSubject = new ClosedOpenInterval(1, 3);
        Assertions.assertThrows(IllegalArgumentException.class, () -> testSubject.getRelativePositionOf(2, 0));
    }

    @Test
    public void testZeroElementInterval() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClosedOpenInterval(3, 3));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("givenTestData")
    public void testCheckForOverlapping(
        final String name, final ClosedOpenInterval interval, final int otherIntervalLowerBoundary, final int otherIntervalHigherBoundary, final ClosedOpenInterval.RelativePosition expectedResult
    ) {
        Assertions.assertEquals(expectedResult, interval.getRelativePositionOf(otherIntervalLowerBoundary, otherIntervalHigherBoundary));
    }

    private static Stream<Arguments> givenTestData() {
        return Stream.of(
                //  Both boundaries are defined
                Arguments.of("Other starts after actual ends", new ClosedOpenInterval(7,  10), 11, 13, ClosedOpenInterval.RelativePosition.AFTER),
                Arguments.of("Other starts where actual ends", new ClosedOpenInterval(7, 10), 10, 13, ClosedOpenInterval.RelativePosition.AFTER),
                Arguments.of("Other starts within actual and ends after actual", new ClosedOpenInterval(7, 10), 9, 13, ClosedOpenInterval.RelativePosition.HEAD_INTERSECTS),
                Arguments.of("Other starts within actual and ends where actual ends", new ClosedOpenInterval(7, 10), 8, 10, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Other is contained by actual", new ClosedOpenInterval(7, 10), 8, 9, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Other starts where actual and ends within actual", new ClosedOpenInterval(7, 10), 7, 12, ClosedOpenInterval.RelativePosition.HEAD_INTERSECTS),
                Arguments.of("Other matches actual", new ClosedOpenInterval(7, 10), 7, 10, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Other starts where actual and finishes within", new ClosedOpenInterval(7, 10), 7, 9, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Other exceeds actual in both directions", new ClosedOpenInterval(7, 10), 6, 12, ClosedOpenInterval.RelativePosition.EXCEEDS),
                Arguments.of("Other starts before actual and ends where actual ends", new ClosedOpenInterval(7, 10), 5, 10, ClosedOpenInterval.RelativePosition.TAIL_INTERSECTS),
                Arguments.of("Other starts before actual and ends within", new ClosedOpenInterval(7, 10), 5, 9, ClosedOpenInterval.RelativePosition.TAIL_INTERSECTS),
                Arguments.of("Other starts where actual ends", new ClosedOpenInterval(7, 10), 2, 7, ClosedOpenInterval.RelativePosition.BEFORE),
                Arguments.of("Other precedes actual interval", new ClosedOpenInterval(7, 10), 2, 6, ClosedOpenInterval.RelativePosition.BEFORE),

                // Actual has no higher boundary defined
                Arguments.of("Fully within with no higher boundary, when start at lower boundary", new ClosedOpenInterval(3, 0), 3, 6, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Fully within with no higher boundary", new ClosedOpenInterval(3, 0), 11, 12, ClosedOpenInterval.RelativePosition.WITHIN),
                Arguments.of("Tail is within with no higher boundary", new ClosedOpenInterval(3, 0), 2, 4, ClosedOpenInterval.RelativePosition.TAIL_INTERSECTS)
        );
    }
}