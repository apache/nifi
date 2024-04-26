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
package org.apache.nifi.processors.slack.consume;


import org.apache.commons.lang3.StringUtils;

public class SlackTimestamp implements Comparable<SlackTimestamp> {
    private final String value;
    private final String normalizedValue;

    public SlackTimestamp() {
        this(System.currentTimeMillis());
    }

    public SlackTimestamp(final long timestamp) {
        final long millis = timestamp % 1000L;
        final long seconds = timestamp / 1000L;
        final String paddedMillis = StringUtils.leftPad(Long.toString(millis), 3, '0');
        this.value = seconds + "." + paddedMillis + "000";
        this.normalizedValue = value;

    }

    public SlackTimestamp(final String value) {
        this.value = value;

        final String[] splits = value.split("\\.");
        final String paddedMicros = StringUtils.rightPad(splits[1], 6, '0');
        normalizedValue = splits[0] + "." + paddedMicros;
    }

    @Override
    public int compareTo(final SlackTimestamp other) {
        if (other == this) {
            return 0;
        }
        return normalizedValue.compareTo(other.normalizedValue);
    }

    public boolean before(final SlackTimestamp other) {
        return compareTo(other) < 0;
    }

    public boolean beforeOrEqualTo(final SlackTimestamp other) {
        return compareTo(other) <= 0;
    }

    public boolean after(final SlackTimestamp other) {
        return compareTo(other) > 0;
    }

    public boolean afterOrEqualTo(final SlackTimestamp other) {
        return compareTo(other) >= 0;
    }

    public String getRawValue() {
        return value;
    }

    public String toString() {
        return normalizedValue;
    }
}
