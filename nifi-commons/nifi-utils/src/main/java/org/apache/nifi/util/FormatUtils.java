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
package org.apache.nifi.util;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormatUtils {
    private static final String UNION = "|";

    // for Data Sizes
    private static final double BYTES_IN_KILOBYTE = 1024;
    private static final double BYTES_IN_MEGABYTE = BYTES_IN_KILOBYTE * 1024;
    private static final double BYTES_IN_GIGABYTE = BYTES_IN_MEGABYTE * 1024;
    private static final double BYTES_IN_TERABYTE = BYTES_IN_GIGABYTE * 1024;

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
     * Formats the specified count by adding commas.
     *
     * @param count the value to add commas to
     * @return the string representation of the given value with commas included
     */
    public static String formatCount(final long count) {
        return NumberFormat.getIntegerInstance().format(count);
    }

    /**
     * Formats the specified duration in 'mm:ss.SSS' format.
     *
     * @param sourceDuration the duration to format
     * @param sourceUnit     the unit to interpret the duration
     * @return representation of the given time data in minutes/seconds
     */
    public static String formatMinutesSeconds(final long sourceDuration, final TimeUnit sourceUnit) {
        final long millis = TimeUnit.MILLISECONDS.convert(sourceDuration, sourceUnit);

        final long millisInMinute = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        final int minutes = (int) (millis / millisInMinute);
        final long secondsMillisLeft = millis - minutes * millisInMinute;

        final long millisInSecond = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
        final int seconds = (int) (secondsMillisLeft / millisInSecond);
        final long millisLeft = secondsMillisLeft - seconds * millisInSecond;

        return pad2Places(minutes) + ":" + pad2Places(seconds) + "." + pad3Places(millisLeft);
    }

    /**
     * Formats the specified duration in 'HH:mm:ss.SSS' format.
     *
     * @param sourceDuration the duration to format
     * @param sourceUnit     the unit to interpret the duration
     * @return representation of the given time data in hours/minutes/seconds
     */
    public static String formatHoursMinutesSeconds(final long sourceDuration, final TimeUnit sourceUnit) {
        final long millis = TimeUnit.MILLISECONDS.convert(sourceDuration, sourceUnit);

        final long millisInHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        final int hours = (int) (millis / millisInHour);
        final long minutesSecondsMillisLeft = millis - hours * millisInHour;

        return pad2Places(hours) + ":" + formatMinutesSeconds(minutesSecondsMillisLeft, TimeUnit.MILLISECONDS);
    }

    private static String pad2Places(final long val) {
        return (val < 10) ? "0" + val : String.valueOf(val);
    }

    private static String pad3Places(final long val) {
        return (val < 100) ? "0" + pad2Places(val) : String.valueOf(val);
    }

    /**
     * Formats the specified data size in human readable format.
     *
     * @param dataSize Data size in bytes
     * @return Human readable format
     */
    public static String formatDataSize(final double dataSize) {
        // initialize the formatter
        final NumberFormat format = NumberFormat.getNumberInstance();
        format.setMaximumFractionDigits(2);

        // check terabytes
        double dataSizeToFormat = dataSize / BYTES_IN_TERABYTE;
        if (dataSizeToFormat > 1) {
            return format.format(dataSizeToFormat) + " TB";
        }

        // check gigabytes
        dataSizeToFormat = dataSize / BYTES_IN_GIGABYTE;
        if (dataSizeToFormat > 1) {
            return format.format(dataSizeToFormat) + " GB";
        }

        // check megabytes
        dataSizeToFormat = dataSize / BYTES_IN_MEGABYTE;
        if (dataSizeToFormat > 1) {
            return format.format(dataSizeToFormat) + " MB";
        }

        // check kilobytes
        dataSizeToFormat = dataSize / BYTES_IN_KILOBYTE;
        if (dataSizeToFormat > 1) {
            return format.format(dataSizeToFormat) + " KB";
        }

        // default to bytes
        return format.format(dataSize) + " bytes";
    }

    /**
     * Returns a time duration in the requested {@link TimeUnit} after parsing the {@code String}
     * input. If the resulting value is a decimal (i.e.
     * {@code 25 hours -> TimeUnit.DAYS = 1.04}), the value is rounded.
     *
     * @param value the raw String input (i.e. "28 minutes")
     * @param desiredUnit the requested output {@link TimeUnit}
     * @return the whole number value of this duration in the requested units
     * @deprecated As of Apache NiFi 1.9.0, because this method only returns whole numbers, use {@link #getPreciseTimeDuration(String, TimeUnit)} when possible.
     */
    @Deprecated
    public static long getTimeDuration(final String value, final TimeUnit desiredUnit) {
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
    public static double getPreciseTimeDuration(final String value, final TimeUnit desiredUnit) {
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
            List wholeResults = makeWholeNumberTime(durationVal, specifiedTimeUnit);
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
    protected static List<Object> makeWholeNumberTime(double decimal, TimeUnit timeUnit) {
        // If the value is already a whole number, return it and the current time unit
        if (decimal == Math.rint(decimal)) {
            return Arrays.asList(new Object[]{(long) decimal, timeUnit});
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
    protected static long calculateMultiplier(TimeUnit originalTimeUnit, TimeUnit newTimeUnit) {
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
    protected static TimeUnit getSmallerTimeUnit(TimeUnit originalUnit) {
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
    protected static boolean isWeek(final String rawUnit) {
        switch (rawUnit) {
            case "w":
            case "wk":
            case "wks":
            case "week":
            case "weeks":
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the {@link TimeUnit} enum that maps to the provided raw {@code String} input. The
     * highest time unit is {@code TimeUnit.DAYS}. Any input that cannot be parsed will result in
     * an {@link IllegalArgumentException}.
     *
     * @param rawUnit the String to parse
     * @return the TimeUnit
     */
    protected static TimeUnit determineTimeUnit(String rawUnit) {
        switch (rawUnit.toLowerCase()) {
            case "ns":
            case "nano":
            case "nanos":
            case "nanoseconds":
                return TimeUnit.NANOSECONDS;
            case "µs":
            case "micro":
            case "micros":
            case "microseconds":
                return TimeUnit.MICROSECONDS;
            case "ms":
            case "milli":
            case "millis":
            case "milliseconds":
                return TimeUnit.MILLISECONDS;
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
                return TimeUnit.SECONDS;
            case "m":
            case "min":
            case "mins":
            case "minute":
            case "minutes":
                return TimeUnit.MINUTES;
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
                return TimeUnit.HOURS;
            case "d":
            case "day":
            case "days":
                return TimeUnit.DAYS;
            default:
                throw new IllegalArgumentException("Could not parse '" + rawUnit + "' to TimeUnit");
        }
    }

    public static String formatUtilization(final double utilization) {
        return utilization + "%";
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

    /**
     * Formats nanoseconds in the format:
     * 3 seconds, 8 millis, 3 nanos - if includeTotalNanos = false,
     * 3 seconds, 8 millis, 3 nanos (3008000003 nanos) - if includeTotalNanos = true
     *
     * @param nanos             the number of nanoseconds to format
     * @param includeTotalNanos whether or not to include the total number of nanoseconds in parentheses in the returned value
     * @return a human-readable String that is a formatted representation of the given number of nanoseconds.
     */
    public static String formatNanos(final long nanos, final boolean includeTotalNanos) {
        final StringBuilder sb = new StringBuilder();

        final long seconds = nanos > 1000000000L ? nanos / 1000000000L : 0L;
        long millis = nanos > 1000000L ? nanos / 1000000L : 0L;
        final long nanosLeft = nanos % 1000000L;

        if (seconds > 0) {
            sb.append(seconds).append(" seconds");
        }
        if (millis > 0) {
            if (seconds > 0) {
                sb.append(", ");
                millis -= seconds * 1000L;
            }

            sb.append(millis).append(" millis");
        }
        if (seconds > 0 || millis > 0) {
            sb.append(", ");
        }
        sb.append(nanosLeft).append(" nanos");

        if (includeTotalNanos) {
            sb.append(" (").append(nanos).append(" nanos)");
        }

        return sb.toString();
    }
}
