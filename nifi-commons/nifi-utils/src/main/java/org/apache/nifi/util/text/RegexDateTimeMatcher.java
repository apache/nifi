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
package org.apache.nifi.util.text;

import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexDateTimeMatcher implements DateTimeMatcher {
    private final Pattern pattern;
    private final List<String> subPatterns;
    private final int minLength;
    private final int maxLength;

    private RegexDateTimeMatcher(final Pattern pattern, final List<String> subPatterns, final int minLength, final int maxLength) {
        this.pattern = pattern;
        this.subPatterns = subPatterns;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public int getMinInputLength() {
        return minLength;
    }

    public int getMaxInputLength() {
        return maxLength;
    }

    @Override
    public boolean matches(final String text) {
        if (text.length() < minLength || text.length() > maxLength) {
            return false;
        }

        return pattern.matcher(text).matches();
    }

    // This method is not used except in order to help debugging. If a pattern is not matching a given input, this can be used
    // to help determine which part of the compiled regular expression is not matching the input
    public String determineMismatch(final String text) {
        for (int patternsToUse = subPatterns.size() - 1; patternsToUse >= 0; patternsToUse--) {
            final StringBuilder sb = new StringBuilder();

            for (int i=0; i < patternsToUse; i++) {
                sb.append(subPatterns.get(i));
            }

            final String regex = "^" + sb.toString();
            final Pattern pattern = Pattern.compile(regex);
            final Matcher matcher = pattern.matcher(text);
            final boolean found = matcher.find();
            if (found) {
                return "Longest Match: <" + matcher.group(0) + "> based on pattern <" + regex + ">. The following portions did not match: " + subPatterns.subList(patternsToUse, subPatterns.size());
            }
        }

        return "Could not match any part of the pattern";
    }


    public static class Compiler {
        private final List<String> patterns = new ArrayList<>();

        private char currentPattern;
        private int charCount;
        private boolean patternStarted = false;

        private static final String AMPM_PATTERN;
        private static final String ERAS_PATTERN;
        private static final String MONTHS_PATTERN;
        private static final String LONG_WEEKDAY_PATTERN;
        private static final String SHORT_WEEKDAY_PATTERN;
        private static final String ZONE_NAME_PATTERN;

        private static final LengthRange AMPM_RANGE;
        private static final LengthRange ERAS_RANGE;
        private static final LengthRange MONTH_NAME_RANGE;
        private static final LengthRange LONG_WEEKDAY_RANGE;
        private static final LengthRange SHORT_WEEKDAY_RANGE;
        private static final LengthRange ZONE_NAME_RANGE;

        private LengthRange range = new LengthRange(0, 0);

        static {
            final DateFormatSymbols dateFormatSymbols = DateFormatSymbols.getInstance(Locale.US);

            final String[] ampm = dateFormatSymbols.getAmPmStrings();
            AMPM_PATTERN = joinRegex(ampm);
            AMPM_RANGE = lengthRange(ampm);

            final String[] eras = dateFormatSymbols.getEras();
            ERAS_PATTERN = joinRegex(eras);
            ERAS_RANGE = lengthRange(eras);

            final List<String> monthNames = new ArrayList<>();
            monthNames.addAll(Arrays.asList(dateFormatSymbols.getMonths()));
            monthNames.addAll(Arrays.asList(dateFormatSymbols.getShortMonths()));
            final String[] monthNameArray = monthNames.toArray(new String[0]);
            MONTHS_PATTERN = joinRegex(monthNameArray);
            MONTH_NAME_RANGE = lengthRange(monthNameArray);

            final String[] longWeekdays = dateFormatSymbols.getWeekdays();
            LONG_WEEKDAY_PATTERN = joinRegex(longWeekdays);
            LONG_WEEKDAY_RANGE = lengthRange(longWeekdays);

            final String[] shortWeekdays = dateFormatSymbols.getShortWeekdays();
            SHORT_WEEKDAY_PATTERN = joinRegex(shortWeekdays);
            SHORT_WEEKDAY_RANGE = lengthRange(shortWeekdays);

            int maxTimeZoneLength = 0;
            final String[][] zoneStrings = dateFormatSymbols.getZoneStrings();
            final StringBuilder zoneNamePatternBuilder = new StringBuilder();
            for (final String[] zoneNames : zoneStrings) {
                for (final String zoneName : zoneNames) {
                    if (zoneName != null && !zoneName.isEmpty()) {
                        zoneNamePatternBuilder.append(Pattern.quote(zoneName)).append("|");
                        maxTimeZoneLength = Math.max(maxTimeZoneLength, zoneName.length());
                    }
                }
            }

            zoneNamePatternBuilder.deleteCharAt(zoneNamePatternBuilder.length() - 1);
            ZONE_NAME_PATTERN = zoneNamePatternBuilder.toString();
            ZONE_NAME_RANGE = new LengthRange(1, maxTimeZoneLength);
        }

        public RegexDateTimeMatcher compile(final String format) {
            currentPattern = 0;
            charCount = 0;

            char lastChar = 0;

            for (int i = 0; i < format.length(); i++) {
                final char c = format.charAt(i);

                if (c != lastChar) {
                    endPattern();
                } else {
                    charCount++;
                }

                try {
                    switch (c) {
                        case '\'':
                            i = copyText(format, i);
                            break;
                        default:
                            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
                                if (c != lastChar) {
                                    beginPattern(c);
                                }

                                continue;
                            }
                            appendChar(c);
                            break;
                    }
                } finally {
                    lastChar = c;
                }
            }

            endPattern();

            final StringBuilder sb = new StringBuilder();
            for (final String pattern : patterns) {
                sb.append(pattern);
            }

            final String regex = sb.toString();
            final Pattern pattern = Pattern.compile(regex);
            return new RegexDateTimeMatcher(pattern, patterns, range.getMinLength(), range.getMaxLength());
        }


        private static LengthRange lengthRange(final String[] values) {
            return new LengthRange(minLength(values), maxLength(values));
        }

        private static int minLength(final String[] values) {
            if (values.length == 0) {
                return 0;
            }

            int minLength = values[0].length();
            for (final String value : values) {
                minLength = Math.min(minLength, value.length());
            }

            return minLength;
        }

        private static int maxLength(final String[] values) {
            if (values.length == 0) {
                return 0;
            }

            int maxLength = values[0].length();
            for (final String value : values) {
                maxLength = Math.max(maxLength, value.length());
            }

            return maxLength;
        }

        private static String joinRegex(final String[] values) {
            final StringBuilder sb = new StringBuilder("(?:");

            for (final String value : values) {
                sb.append(Pattern.quote(value)).append("|");
            }

            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        }

        private int copyText(final String formatString, final int startChar) {
            boolean lastCharQuote = false;

            final StringBuilder textBuilder = new StringBuilder();

            try {
                for (int i = startChar + 1; i < formatString.length(); i++) {
                    final char c = formatString.charAt(i);
                    if (c == '\'') {
                        // We found a quote char. If the last character is also a quote, then it was an escape character. Copy a single quote, set lastCharQuote = false because we've finished
                        // the escape sequence, and then continue to the next character.
                        if (lastCharQuote) {
                            textBuilder.append("'");
                            lastCharQuote = false;
                            continue;
                        }

                        // We found a quote character. The last character is not a quote. This character may or may not be an escape character, so we have to move on to the next character to find out.
                        lastCharQuote = true;
                        continue;
                    } else if (lastCharQuote) {
                        // The current character is not a quote character but the last character was. This means that the last character was ending the quotation.
                        return i - 1;
                    }

                    textBuilder.append(c);
                    lastCharQuote = false;
                }

                return formatString.length();
            } finally {
                if (textBuilder.length() == 0) {
                    patterns.add("'");
                } else {
                    final String text = textBuilder.toString();
                    if (text.length() > 0) {
                        patterns.add(Pattern.quote(textBuilder.toString()));
                    }
                }
            }
        }

        private void beginPattern(final char c) {
            this.patternStarted = true;
            this.charCount = 1;
            this.currentPattern = c;
        }

        private void appendChar(final char c) {
            patterns.add(Pattern.quote(String.valueOf(c)));
            range = range.plus(1, 1);
        }

        private void endPattern() {
            if (!patternStarted) {
                return;
            }

            patternStarted = false;
            switch (currentPattern) {
                case 'G':
                    addEraDesignator();
                    break;
                case 'y':
                case 'Y':
                    if (this.charCount == 2) {
                        addYear(2);
                    } else {
                        addYear(this.charCount);
                    }
                    break;
                case 'M':
                    if (this.charCount <= 2) {
                        addShortMonth();
                    } else {
                        addLongMonth();
                    }
                    break;
                case 'w':
                    addWeekInYear();
                    break;
                case 'W':
                    addWeekInMonth();
                    break;
                case 'D':
                    addDayInYear();
                    break;
                case 'd':
                    addDayInMonth();
                    break;
                case 'F':
                    addDayOfWeekInMonth();
                    break;
                case 'E':
                    if (this.charCount <= 3) {
                        addShortDayNameInWeek();
                    } else {
                        addLongDayNameInWeek();
                    }
                    break;
                case 'u':
                    addDayNumberInWeek();
                    break;
                case 'a':
                    addAmPmMarker();
                    break;
                case 'H':
                    addHourInDayBaseZero();
                    break;
                case 'k':
                    addHourInDayBaseOne();
                    break;
                case 'K':
                    add12HourBaseZero();
                    break;
                case 'h':
                    add12HourBaseOne();
                    break;
                case 'm':
                    addMinuteInHour();
                    break;
                case 's':
                    addSecondInMinute();
                    break;
                case 'S':
                    addMillisecond();
                    break;
                case 'z':
                    addGeneralTimeZone();
                    break;
                case 'Z':
                    addRFC822TimeZone();
                    break;
                case 'X':
                    addISO8601TimeZone();
                    break;
            }
        }

        private void addEraDesignator() {
            patterns.add(ERAS_PATTERN);
            range = range.plus(ERAS_RANGE);
        }

        private void addYear(final int maxDigits) {
            patterns.add("(?:-?\\d{1," + maxDigits + "})");
            range = range.plus(1, maxDigits);
        }

        private void addShortMonth() {
            patterns.add("(?:0[1-9]|1[0-2])");
            range = range.plus(1, 2);
        }

        private void addLongMonth() {
            patterns.add("(?:" + MONTHS_PATTERN + ")");
            range = range.plus(MONTH_NAME_RANGE);
        }

        private void addWeekInYear() {
            patterns.add("\\d{1,2}");
            range = range.plus(1, 4);
        }

        private void addWeekInMonth() {
            patterns.add("[0-4]");
            range = range.plus(1, 1);
        }

        private void addDayInYear() {
            patterns.add("\\d{1,3}");
            range = range.plus(1, 3);
        }

        private void addDayInMonth() {
            patterns.add("[0-3]?[0-9]");
            range = range.plus(1, 2);
        }

        private void addDayOfWeekInMonth() {
            patterns.add("[0-7]");
            range = range.plus(1, 1);
        }

        private void addShortDayNameInWeek() {
            patterns.add(SHORT_WEEKDAY_PATTERN);
            range = range.plus(SHORT_WEEKDAY_RANGE);
        }

        private void addLongDayNameInWeek() {
            patterns.add(LONG_WEEKDAY_PATTERN);
            range = range.plus(LONG_WEEKDAY_RANGE);
        }

        private void addDayNumberInWeek() {
            patterns.add("[1-7]");
            range = range.plus(1, 1);
        }

        private void addAmPmMarker() {
            patterns.add(AMPM_PATTERN);
            range = range.plus(AMPM_RANGE);
        }

        private void addHourInDayBaseZero() {
            patterns.add("(?:[0-9]|[01][0-9]|2[0-3])");
            range = range.plus(1, 2);
        }

        private void addHourInDayBaseOne() {
            patterns.add("(?:[1-9]|0[1-9]|1[0-9]|2[0-4])");
            range = range.plus(1, 2);
        }

        private void add12HourBaseZero() {
            patterns.add("(?:0?[0-9]|1[01])");
            range = range.plus(1, 2);
        }

        private void add12HourBaseOne() {
            patterns.add("(?:[1-9]|0[1-9]|1[012])");
            range = range.plus(1, 2);
        }

        private void addMinuteInHour() {
            patterns.add("(?:[0-9]|[0-5][0-9])");
            range = range.plus(1, 2);
        }

        private void addSecondInMinute() {
            addMinuteInHour(); // Same pattern
            range = range.plus(1, 2);
        }

        private void addMillisecond() {
            patterns.add("\\d{1,3}");
            range = range.plus(1, 3);
        }

        private void addGeneralTimeZone() {
            final StringBuilder sb = new StringBuilder();

            sb.append("(?:"); // begin non-capturing group
            sb.append(getGMTOffsetTimeZone());
            sb.append("|");
            sb.append(getNamedTimeZone());
            sb.append(")"); // end non-capturing group

            patterns.add(sb.toString());
            range = range.plus(ZONE_NAME_RANGE);
        }

        private String getGMTOffsetTimeZone() {
            // From SimpleDateFormat JavaDocs, GMTOffsetTimeZone defined as: GMT Sign Hours : Minutes
            // Sign defined as '-' or '+'
            // Hours defined as 1 or 2 digits, Minutes defined as 1 or 2 digits
            // Digit defined as number between 0-9
            return "(?:GMT[-+]\\d{1,2}:\\d{2})";
        }

        private String getNamedTimeZone() {
            return ZONE_NAME_PATTERN;
        }

        private void addRFC822TimeZone() {
            patterns.add("(?:[-+]\\d{4})");
            range = range.plus(5, 5);
        }

        private void addISO8601TimeZone() {
            patterns.add("(?:Z|(?:[-+](?:\\d{2}|\\d{4}|\\d{2}\\:\\d{2})))");
            range = range.plus(1, 6);
        }


        private static class LengthRange {
            private final int min;
            private final int max;

            public LengthRange(final int min, final int max) {
                this.min = min;
                this.max = max;
            }

            public int getMinLength() {
                return min;
            }

            public int getMaxLength() {
                return max;
            }

            public LengthRange plus(final LengthRange other) {
                return new LengthRange(getMinLength() + other.getMinLength(), getMaxLength() + other.getMaxLength());
            }

            public LengthRange plus(final int min, final int max) {
                return new LengthRange(getMinLength() + min, getMaxLength() + max);
            }

        }
    }
}
