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

package org.apache.nifi.toolkit.tls.configuration;

import org.apache.nifi.util.StringUtils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Each instance is uniquely identified by its hostname and instance number
 */
public class InstanceIdentifier {
    public static final Comparator<InstanceIdentifier> HOST_IDENTIFIER_COMPARATOR = (o1, o2) -> {
        int i = o1.getHostname().compareTo(o2.getHostname());
        if (i == 0) {
            return o1.getNumber() - o2.getNumber();
        }
        return i;
    };
    private static final Pattern RANGE_PATTERN = Pattern.compile("^[0-9]+(-[0-9]+)?$");
    private final String hostname;
    private final int number;

    public InstanceIdentifier(String hostname, int number) {
        this.hostname = hostname;
        this.number = number;
    }

    /**
     * Creates a map that can be used to deterministically assign global instance numbers
     *
     * @param hostnameExpressions the hostname expressions to expand
     * @return a map that can be used to deterministically assign global instance numbers
     */
    public static Map<InstanceIdentifier, Integer> createOrderMap(Stream<String> hostnameExpressions) {
        List<InstanceIdentifier> instanceIdentifiers = createIdentifiers(hostnameExpressions).sorted(HOST_IDENTIFIER_COMPARATOR).collect(Collectors.toList());
        Map<InstanceIdentifier, Integer> result = new HashMap<>();
        for (int i = 0; i < instanceIdentifiers.size(); i++) {
            result.put(instanceIdentifiers.get(i), i + 1);
        }
        return result;
    }

    /**
     * Creates a stream of hostname identifiers from a stream of hostname expressions
     *
     * @param hostnameExpressions the hostname expressions
     * @return the hostname identifiers
     */
    public static Stream<InstanceIdentifier> createIdentifiers(Stream<String> hostnameExpressions) {
        return hostnameExpressions.flatMap(hostnameExpression -> extractHostnames(hostnameExpression).flatMap(hostname -> {
            ExtractedRange extractedRange = new ExtractedRange(hostname, '(', ')');
            if (extractedRange.range == null) {
                return Stream.of(new InstanceIdentifier(hostname, 1));
            }
            if (!StringUtils.isEmpty(extractedRange.afterClose)) {
                throw new IllegalArgumentException("No characters expected after )");
            }
            return extractedRange.range.map(numString -> new InstanceIdentifier(extractedRange.beforeOpen, Integer.parseInt(numString)));
        }));
    }

    protected static Stream<String> extractHostnames(String hostname) {
        ExtractedRange extractedRange = new ExtractedRange(hostname, '[', ']');
        if (extractedRange.range == null) {
            return Stream.of(hostname);
        }
        return extractedRange.range.map(s -> extractedRange.beforeOpen + s + extractedRange.afterClose).flatMap(InstanceIdentifier::extractHostnames);
    }

    private static Stream<String> extractRange(String range) {
        if (!RANGE_PATTERN.matcher(range).matches()) {
            throw new IllegalArgumentException("Expected either one number or two separated by a single hyphen");
        }
        String[] split = range.split("-");
        if (split.length == 1) {
            String prefix = "1-";
            if (split[0].charAt(0) == '0') {
                prefix = String.format("%0" + split[0].length() + "d-", 1);
            }
            return extractRange(prefix + split[0]);
        } else {
            int baseLength = split[0].length();
            int low = Integer.parseInt(split[0]);
            String padding = split[0].substring(0, split[0].length() - Integer.toString(low).length());
            int high = Integer.parseInt(split[1]);
            return IntStream.range(low, high + 1).mapToObj(i -> {
                String s = Integer.toString(i);
                int length = s.length();
                if (length >= baseLength) {
                    return s;
                } else {
                    return padding.substring(0, baseLength - length) + s;
                }
            });
        }
    }

    public String getHostname() {
        return hostname;
    }

    public int getNumber() {
        return number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstanceIdentifier that = (InstanceIdentifier) o;

        if (number != that.number) return false;
        return hostname != null ? hostname.equals(that.hostname) : that.hostname == null;

    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + number;
        return result;
    }

    private static class ExtractedRange {
        private final String beforeOpen;
        private final Stream<String> range;
        private final String afterClose;

        public ExtractedRange(String string, char rangeOpen, char rangeClose) {
            int openBracket = string.indexOf(rangeOpen);
            if (openBracket >= 0) {
                int closeBracket = string.indexOf(rangeClose, openBracket);
                if (closeBracket < 0) {
                    throw new IllegalArgumentException("Unable to find matching " + rangeClose + " for " + rangeOpen + " in " + string);
                }
                beforeOpen = string.substring(0, openBracket);
                if (closeBracket + 1 < string.length()) {
                    afterClose = string.substring(closeBracket + 1);
                } else {
                    afterClose = "";
                }
                range = extractRange(string.substring(openBracket + 1, closeBracket));
            } else {
                beforeOpen = string;
                range = null;
                afterClose = "";
            }
        }
    }
}
