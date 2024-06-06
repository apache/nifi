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

public interface Interval {

    enum RelativePosition {
        /**
         * The compared interval ends before the actual, there is no intersection.
         */
        BEFORE,

        /**
         * The compared interval exceeds the actual both at the low and high ends.
         */
        EXCEEDS,

        /**
         * The compared interval's tail (but not the whole interval) intersects the actual interval (part of it or the whole actual interval).
         */
        TAIL_INTERSECTS,

        /**
         * The compared interval is within the actual interval. It can match with the actual or contained by that.
         */
        WITHIN,

        /**
         *The compared interval's head (but not the whole interval) intersects the actual interval  (part of it or the whole actual interval).
         */
        HEAD_INTERSECTS,

        /**
         * The compared interval starts after the actual, there is no intersection.
         */
        AFTER,
    }

    /**
     * Relative position of the "other" interval compared to this.
     *
     * @param otherIntervalLowerBoundary Lower boundary of the compared interval.
     * @param otherIntervalHigherBoundary Higher boundary of the compared interval.
     *
     * @return Returns the relative position of the "other" interval compared to this interval. For example: if the result
     *         is BEFORE, read it as: the other interval ends BEFORE the actual (and there is no intersection between them).
     */
    RelativePosition getRelativePositionOf(final int otherIntervalLowerBoundary, final int otherIntervalHigherBoundary);
}
