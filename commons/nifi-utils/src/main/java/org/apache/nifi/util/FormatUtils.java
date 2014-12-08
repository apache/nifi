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
import java.text.SimpleDateFormat;
import java.util.Date;
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
    private static final String NANOS = join(UNION, "ns", "nano", "nanos", "nanoseconds");
    private static final String MILLIS = join(UNION, "ms", "milli", "millis", "milliseconds");
    private static final String SECS = join(UNION, "s", "sec", "secs", "second", "seconds");
    private static final String MINS = join(UNION, "m", "min", "mins", "minute", "minutes");
    private static final String HOURS = join(UNION, "h", "hr", "hrs", "hour", "hours");
    private static final String DAYS = join(UNION, "d", "day", "days");

    private static final String VALID_TIME_UNITS = join(UNION, NANOS, MILLIS, SECS, MINS, HOURS, DAYS);
    public static final String TIME_DURATION_REGEX = "(\\d+)\\s*(" + VALID_TIME_UNITS + ")";
    public static final Pattern TIME_DURATION_PATTERN = Pattern.compile(TIME_DURATION_REGEX);

    /**
     * Formats the specified count by adding commas.
     *
     * @param count
     * @return
     */
    public static String formatCount(final long count) {
        return NumberFormat.getIntegerInstance().format(count);
    }

    /**
     * Formats the specified duration in 'mm:ss.SSS' format.
     *
     * @param sourceDuration
     * @param sourceUnit
     * @return
     */
    public static String formatMinutesSeconds(final long sourceDuration, final TimeUnit sourceUnit) {
        final long millis = TimeUnit.MILLISECONDS.convert(sourceDuration, sourceUnit);
        final SimpleDateFormat formatter = new SimpleDateFormat("mm:ss.SSS");
        return formatter.format(new Date(millis));
    }

    /**
     * Formats the specified duration in 'HH:mm:ss.SSS' format.
     *
     * @param sourceDuration
     * @param sourceUnit
     * @return
     */
    public static String formatHoursMinutesSeconds(final long sourceDuration, final TimeUnit sourceUnit) {
        final long millis = TimeUnit.MILLISECONDS.convert(sourceDuration, sourceUnit);
        final long millisInHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        final int hours = (int) (millis / millisInHour);
        final long whatsLeft = millis - hours * millisInHour;

        return pad(hours) + ":" + new SimpleDateFormat("mm:ss.SSS").format(new Date(whatsLeft));
    }

    private static String pad(final int val) {
        return (val < 10) ? "0" + val : String.valueOf(val);
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

}
