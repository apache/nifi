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

package org.apache.nifi.time;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeFormat {
    private static final String UNION = "|";


    // for Time Durations
    private static final String NANOS = join(UNION, "ns", "nano", "nanos", "nanosecond", "nanoseconds");
    private static final String MILLIS = join(UNION, "ms", "milli", "millis", "millisecond", "milliseconds");
    private static final String SECS = join(UNION, "s", "sec", "secs", "second", "seconds");
    private static final String MINS = join(UNION, "m", "min", "mins", "minute", "minutes");
    private static final String HOURS = join(UNION, "h", "hr", "hrs", "hour", "hours");
    private static final String DAYS = join(UNION, "d", "day", "days");
    private static final String WEEKS = join(UNION, "w", "wk", "wks", "week", "weeks");

    private static final String VALID_TIME_UNITS = join(UNION, NANOS, MILLIS, SECS, MINS, HOURS, DAYS, WEEKS);
    public static final String TIME_DURATION_REGEX = "([\\d.]+)\\s*(" + VALID_TIME_UNITS + ")";
    public static final Pattern TIME_DURATION_PATTERN = Pattern.compile(TIME_DURATION_REGEX);
    private static final List<Long> TIME_UNIT_MULTIPLIERS = Arrays.asList(1000L, 1000L, 1000L, 60L, 60L, 24L);


    /**
     * Returns a time duration in the requested {@link TimeUnit} after parsing the {@code String}
     * input. If the resulting value is a decimal (i.e.
     * {@code 25 hours -> TimeUnit.DAYS = 1.04}), the value is rounded.
     * Use {@link #getPreciseTimeDuration(String, TimeUnit)} if fractional values are desirable
     *
     * @param value the raw String input (i.e. "28 minutes")
     * @param desiredUnit the requested output {@link TimeUnit}
     * @return the whole number value of this duration in the requested units
     * @see #getPreciseTimeDuration(String, TimeUnit)
     */
    public long getTimeDuration(final String value, final TimeUnit desiredUnit) {
        return Math.round(getPreciseTimeDuration(value, desiredUnit));
    }

    /**
     * Returns the parsed and converted input in the requested units.
     * <p>
     * If the value is {@code 0 <= x < 1} in the provided units, the units will first be converted to a smaller unit to get a value >= 1 (i.e. 0.5 seconds -> 500 milliseconds).
     * This is because the underlying unit conversion cannot handle decimal values.
     * <p>
     * If the value is {@code x >= 1} but x is not a whole number, the units will first be converted to a smaller unit to attempt to get a whole number value (i.e. 1.5 seconds -> 1500 milliseconds).
     * <p>
     * If the value is {@code x < 1000} and the units are {@code TimeUnit.NANOSECONDS}, the result will be a whole number of nanoseconds, rounded (i.e. 123.4 ns -> 123 ns).
     * <p>
     * This method handles decimal values over {@code 1 ns}, but {@code < 1 ns} will return {@code 0} in any other unit.
     * <p>
     * Examples:
     * <p>
     * "10 seconds", {@code TimeUnit.MILLISECONDS} -> 10_000.0
     * "0.010 s", {@code TimeUnit.MILLISECONDS} -> 10.0
     * "0.010 s", {@code TimeUnit.SECONDS} -> 0.010
     * "0.010 ns", {@code TimeUnit.NANOSECONDS} -> 1
     * "0.010 ns", {@code TimeUnit.MICROSECONDS} -> 0
     *
     * @param value       the {@code String} input
     * @param desiredUnit the desired output {@link TimeUnit}
     * @return the parsed and converted amount (without a unit)
     */
    public double getPreciseTimeDuration(final String value, final TimeUnit desiredUnit) {
        final Matcher matcher = TIME_DURATION_PATTERN.matcher(value.toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Value '" + value + "' is not a valid time duration");
        }

        final String duration = matcher.group(1);
        final String units = matcher.group(2);

        double durationVal = Double.parseDouble(duration);
        TimeUnit specifiedTimeUnit;

        // The TimeUnit enum doesn't have a value for WEEKS, so handle this case independently
        if (isWeek(units)) {
            specifiedTimeUnit = TimeUnit.DAYS;
            durationVal *= 7;
        } else {
            specifiedTimeUnit = determineTimeUnit(units);
        }

        // The units are now guaranteed to be in DAYS or smaller
        long durationLong;
        if (durationVal == Math.rint(durationVal)) {
            durationLong = Math.round(durationVal);
        } else {
            // Try reducing the size of the units to make the input a long
            List<?> wholeResults = makeWholeNumberTime(durationVal, specifiedTimeUnit);
            durationLong = (long) wholeResults.get(0);
            specifiedTimeUnit = (TimeUnit) wholeResults.get(1);
        }

        return desiredUnit.convert(durationLong, specifiedTimeUnit);
    }

    /**
     * Converts the provided time duration value to one that can be represented as a whole number.
     * Returns a {@code List} containing the new value as a {@code long} at index 0 and the
     * {@link TimeUnit} at index 1. If the incoming value is already whole, it is returned as is.
     * If the incoming value cannot be made whole, a whole approximation is returned. For values
     * {@code >= 1 TimeUnit.NANOSECONDS}, the value is rounded (i.e. 123.4 ns -> 123 ns).
     * For values {@code < 1 TimeUnit.NANOSECONDS}, the constant [1L, {@code TimeUnit.NANOSECONDS}] is returned as the smallest measurable unit of time.
     * <p>
     * Examples:
     * <p>
     * 1, {@code TimeUnit.SECONDS} -> [1, {@code TimeUnit.SECONDS}]
     * 1.1, {@code TimeUnit.SECONDS} -> [1100, {@code TimeUnit.MILLISECONDS}]
     * 0.1, {@code TimeUnit.SECONDS} -> [100, {@code TimeUnit.MILLISECONDS}]
     * 0.1, {@code TimeUnit.NANOSECONDS} -> [1, {@code TimeUnit.NANOSECONDS}]
     *
     * @param decimal  the time duration as a decimal
     * @param timeUnit the current time unit
     * @return the time duration as a whole number ({@code long}) and the smaller time unit used
     */
    List<Object> makeWholeNumberTime(double decimal, TimeUnit timeUnit) {
        // If the value is already a whole number, return it and the current time unit
        if (decimal == Math.rint(decimal)) {
            final long rounded = Math.round(decimal);
            return Arrays.asList(new Object[]{rounded, timeUnit});
        } else if (TimeUnit.NANOSECONDS == timeUnit) {
            // The time unit is as small as possible
            if (decimal < 1.0) {
                decimal = 1;
            } else {
                decimal = Math.rint(decimal);
            }
            return Arrays.asList(new Object[]{(long) decimal, timeUnit});
        } else {
            // Determine the next time unit and the respective multiplier
            TimeUnit smallerTimeUnit = getSmallerTimeUnit(timeUnit);
            long multiplier = calculateMultiplier(timeUnit, smallerTimeUnit);

            // Recurse with the original number converted to the smaller unit
            return makeWholeNumberTime(decimal * multiplier, smallerTimeUnit);
        }
    }

    /**
     * Returns the numerical multiplier to convert a value from {@code originalTimeUnit} to
     * {@code newTimeUnit} (i.e. for {@code TimeUnit.DAYS -> TimeUnit.MINUTES} would return
     * 24 * 60 = 1440). If the original and new units are the same, returns 1. If the new unit
     * is larger than the original (i.e. the result would be less than 1), throws an
     * {@link IllegalArgumentException}.
     *
     * @param originalTimeUnit the source time unit
     * @param newTimeUnit      the destination time unit
     * @return the numerical multiplier between the units
     */
    long calculateMultiplier(TimeUnit originalTimeUnit, TimeUnit newTimeUnit) {
        if (originalTimeUnit == newTimeUnit) {
            return 1;
        } else if (originalTimeUnit.ordinal() < newTimeUnit.ordinal()) {
            throw new IllegalArgumentException("The original time unit '" + originalTimeUnit + "' must be larger than the new time unit '" + newTimeUnit + "'");
        } else {
            int originalOrd = originalTimeUnit.ordinal();
            int newOrd = newTimeUnit.ordinal();

            List<Long> unitMultipliers = TIME_UNIT_MULTIPLIERS.subList(newOrd, originalOrd);
            return unitMultipliers.stream().reduce(1L, (a, b) -> (long) a * b);
        }
    }

    /**
     * Returns the next smallest {@link TimeUnit} (i.e. {@code TimeUnit.DAYS -> TimeUnit.HOURS}).
     * If the parameter is {@code null} or {@code TimeUnit.NANOSECONDS}, an
     * {@link IllegalArgumentException} is thrown because there is no valid smaller TimeUnit.
     *
     * @param originalUnit the TimeUnit
     * @return the next smaller TimeUnit
     */
    TimeUnit getSmallerTimeUnit(TimeUnit originalUnit) {
        if (originalUnit == null || TimeUnit.NANOSECONDS == originalUnit) {
            throw new IllegalArgumentException("Cannot determine a smaller time unit than '" + originalUnit + "'");
        } else {
            return TimeUnit.values()[originalUnit.ordinal() - 1];
        }
    }

    /**
     * Returns {@code true} if this raw unit {@code String} is parsed as representing "weeks", which does not have a value in the {@link TimeUnit} enum.
     *
     * @param rawUnit the String containing the desired unit
     * @return true if the unit is "weeks"; false otherwise
     */
    private boolean isWeek(final String rawUnit) {
        return switch (rawUnit) {
            case "w", "wk", "wks", "week", "weeks" -> true;
            default -> false;
        };
    }

    /**
     * Returns the {@link TimeUnit} enum that maps to the provided raw {@code String} input. The
     * highest time unit is {@code TimeUnit.DAYS}. Any input that cannot be parsed will result in
     * an {@link IllegalArgumentException}.
     *
     * @param rawUnit the String to parse
     * @return the TimeUnit
     */
    protected TimeUnit determineTimeUnit(String rawUnit) {
        return switch (rawUnit.toLowerCase()) {
            case "ns", "nano", "nanos", "nanoseconds" -> TimeUnit.NANOSECONDS;
            case "µs", "micro", "micros", "microseconds" -> TimeUnit.MICROSECONDS;
            case "ms", "milli", "millis", "milliseconds" -> TimeUnit.MILLISECONDS;
            case "s", "sec", "secs", "second", "seconds" -> TimeUnit.SECONDS;
            case "m", "min", "mins", "minute", "minutes" -> TimeUnit.MINUTES;
            case "h", "hr", "hrs", "hour", "hours" -> TimeUnit.HOURS;
            case "d", "day", "days" -> TimeUnit.DAYS;
            default -> throw new IllegalArgumentException("Could not parse '" + rawUnit + "' to TimeUnit");
        };
    }

    private static String join(final String delimiter, final String... values) {
        if (values.length == 0) {
            return "";
        } else if (values.length == 1) {
            return values[0];
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(values[0]);
        for (int i = 1; i < values.length; i++) {
            sb.append(delimiter).append(values[i]);
        }

        return sb.toString();
    }

}
