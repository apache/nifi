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

/**
 * This implementation includes the lower boundary but does not include the higher boundary.
 */
final class ClosedOpenInterval implements Interval {
    private final int lowerBoundary;
    private final int higherBoundary;

    /**
     * @param lowerBoundary Inclusive index of lower boundary
     * @param higherBoundary Exclusive index of higher boundary. In case of 0, the higher boundary is unspecified and the interval is open.
     */
    ClosedOpenInterval(final int lowerBoundary, final int higherBoundary) {
        if (lowerBoundary < 0) {
            throw new IllegalArgumentException("Lower boundary cannot be negative");
        }

        if (higherBoundary < 0) {
            throw new IllegalArgumentException("Higher boundary cannot be negative");
        }

        if (higherBoundary <= lowerBoundary && higherBoundary != 0) {
            throw new IllegalArgumentException(
                "Higher boundary cannot be lower than or equal to lower boundary except when unspecified. Higher boundary is considered unspecified when the value is set to 0"
            );
        }

        this.lowerBoundary = lowerBoundary;
        this.higherBoundary = higherBoundary;
    }

    @Override
    public RelativePosition getRelativePositionOf(final int otherIntervalLowerBoundary, final int otherIntervalHigherBoundary) {
        if (otherIntervalLowerBoundary < 0) {
            throw new IllegalArgumentException("Lower boundary cannot be negative");
        }

        if (otherIntervalHigherBoundary <= 0) {
            // Note: as a design decision the implementation currently does not support comparison with unspecified higher boundary
            throw new IllegalArgumentException("Higher boundary must be positive");
        }

        if (otherIntervalLowerBoundary >= otherIntervalHigherBoundary) {
            throw new IllegalArgumentException("Higher boundary must be greater than lower boundary");
        }

        if (otherIntervalHigherBoundary <= lowerBoundary) {
            return RelativePosition.BEFORE;
        } else if (otherIntervalLowerBoundary < lowerBoundary && otherIntervalHigherBoundary > higherBoundary && !this.isEndUnspecified()) {
            return RelativePosition.EXCEEDS;
        } else if (otherIntervalLowerBoundary < lowerBoundary) {
            return RelativePosition.TAIL_INTERSECTS;
        } else if (otherIntervalHigherBoundary <= higherBoundary || this.isEndUnspecified()) {
            return RelativePosition.WITHIN;
        } else if (otherIntervalLowerBoundary < higherBoundary) {
            return RelativePosition.HEAD_INTERSECTS;
        } else {
            return RelativePosition.AFTER;
        }
    }

    private boolean isEndUnspecified() {
        return higherBoundary == 0;
    }
}
