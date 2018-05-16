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
    public static final String TIME_DURATION_REGEX = "(\\d+)\\s*(" + VALID_TIME_UNITS + ")";
    public static final Pattern TIME_DURATION_PATTERN = Pattern.compile(TIME_DURATION_REGEX);

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
     * @param sourceUnit the unit to interpret the duration
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
     * @param sourceUnit the unit to interpret the duration
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

    public static long getTimeDuration(final String value, final TimeUnit desiredUnit) {
        final Matcher matcher = TIME_DURATION_PATTERN.matcher(value.toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Value '" + value + "' is not a valid Time Duration");
        }

        final String duration = matcher.group(1);
        final String units = matcher.group(2);
        TimeUnit specifiedTimeUnit = null;
        switch (units.toLowerCase()) {
            case "ns":
            case "nano":
            case "nanos":
            case "nanoseconds":
                specifiedTimeUnit = TimeUnit.NANOSECONDS;
                break;
            case "ms":
            case "milli":
            case "millis":
            case "milliseconds":
                specifiedTimeUnit = TimeUnit.MILLISECONDS;
                break;
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
                specifiedTimeUnit = TimeUnit.SECONDS;
                break;
            case "m":
            case "min":
            case "mins":
            case "minute":
            case "minutes":
                specifiedTimeUnit = TimeUnit.MINUTES;
                break;
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
                specifiedTimeUnit = TimeUnit.HOURS;
                break;
            case "d":
            case "day":
            case "days":
                specifiedTimeUnit = TimeUnit.DAYS;
                break;
            case "w":
            case "wk":
            case "wks":
            case "week":
            case "weeks":
                final long durationVal = Long.parseLong(duration);
                return desiredUnit.convert(durationVal, TimeUnit.DAYS)*7;
        }

        final long durationVal = Long.parseLong(duration);
        return desiredUnit.convert(durationVal, specifiedTimeUnit);
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
     * @param nanos the number of nanoseconds to format
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
