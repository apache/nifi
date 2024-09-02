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
package org.apache.nifi.serialization.record.util;

import java.time.Instant;

public class FractionalSecondsUtils {
    private static final long YEAR_TEN_THOUSAND = 253_402_300_800_000L;

    private FractionalSecondsUtils() {
    }

    public static Instant tryParseAsNumber(final String value) {
        // If decimal, treat as a double and convert to seconds and nanoseconds.
        if (value.contains(".")) {
            final double number = Double.parseDouble(value);
            return toInstant(number);
        }

        // attempt to parse as a long value
        final long number = Long.parseLong(value);
        return toInstant(number);
    }

    public static Instant toInstant(final double secondsSinceEpoch) {
        // Determine the number of micros past the second by subtracting the number of seconds from the decimal value and multiplying by 1 million.
        final double micros = 1_000_000 * (secondsSinceEpoch - (long) secondsSinceEpoch);
        // Convert micros to nanos. Note that we perform this as a separate operation, rather than multiplying by 1_000,000,000 in order to avoid
        // issues that occur with rounding at high precision.
        final long nanos = (long) micros * 1000L;

        return Instant.ofEpochSecond((long) secondsSinceEpoch).plusNanos(nanos);
    }

    public static Instant toInstant(final long value) {
        // Value is too large. Assume microseconds instead of milliseconds.
        if (value > YEAR_TEN_THOUSAND) {
           return Instant.ofEpochSecond(value / 1_000_000, (value % 1_000_000) * 1_000);
        }

        return Instant.ofEpochMilli(value);
    }
}
