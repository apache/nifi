package org.apache.nifi.serialization.record.util;

import java.time.Instant;

public class InstantFieldUtils {
    private static final long YEAR_TEN_THOUSAND = 253_402_300_800_000L;

    private InstantFieldUtils() {}

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
