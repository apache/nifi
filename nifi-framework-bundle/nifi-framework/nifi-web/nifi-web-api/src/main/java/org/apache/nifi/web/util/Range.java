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

public class Range {
    private final int lowerBoundary;
    private final int higherBoundary;

    /**
     * @param lowerBoundary Inclusive index of lower boundary
     * @param higherBoundary Exclusive index of higher boundary. In case of 0, the higher boundary is unspecified and the range is open.
     */
    public Range(final int lowerBoundary, final int higherBoundary) {
        if (lowerBoundary < 0) {
            throw new IllegalArgumentException("Lower boundary cannot be negative");
        }

        if (higherBoundary < 0) {
            throw new IllegalArgumentException("Higher boundary cannot be negative");
        }

        if (higherBoundary <= lowerBoundary && higherBoundary != 0) {
            throw new IllegalArgumentException("Higher boundary cannot be lower or equal to lower boundary expect when unspecified");
        }

        this.lowerBoundary = lowerBoundary;
        this.higherBoundary = higherBoundary;
    }

    public RelativePosition getOverlapping(final int otherRangeLowerBoundary, final int otherRangeHigherBoundary) {
        if (otherRangeHigherBoundary < lowerBoundary) {
            return RelativePosition.BEFORE_RANGE;
        } else if (otherRangeLowerBoundary < lowerBoundary && otherRangeHigherBoundary >= higherBoundary && !isOpenEnded()) {
            return RelativePosition.MIDDLE_IS_WITHIN_RANGE;
        } else if (otherRangeLowerBoundary < lowerBoundary) {
            return RelativePosition.TAIL_IS_WITHIN_RANGE;
        } else if (otherRangeHigherBoundary < higherBoundary || isOpenEnded()) {
            return RelativePosition.FULLY_WITHIN_RANGE;
        } else if (otherRangeLowerBoundary < higherBoundary) {
            return RelativePosition.HEAD_IS_WITHIN_RANGE;
        } else {
            return RelativePosition.AFTER_RANGE;
        }
    }

    private boolean isOpenEnded() {
        return higherBoundary == 0;
    }

    public enum RelativePosition {
        BEFORE_RANGE,
        MIDDLE_IS_WITHIN_RANGE,
        TAIL_IS_WITHIN_RANGE,
        FULLY_WITHIN_RANGE,
        HEAD_IS_WITHIN_RANGE,
        AFTER_RANGE,
    }
}
