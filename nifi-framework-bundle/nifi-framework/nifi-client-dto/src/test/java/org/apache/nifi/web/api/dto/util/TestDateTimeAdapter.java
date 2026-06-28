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
package org.apache.nifi.web.api.dto.util;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDateTimeAdapter {

    private static final DateTimeAdapter DEFAULT_TIME_ZONE_ADAPTER = new DateTimeAdapter();
    // Month, day, hour, minute, and second values are distinct for verification.
    private static final long TEST_DATE_TIME_MILLISECONDS = 1767323096000L; // Represents 2026-01-02T03:04:56Z
    private static final Date TEST_DATE = new Date(TEST_DATE_TIME_MILLISECONDS);
    private static final DateTimeFormatter TEST_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss z")
            .withZone(ZoneId.systemDefault());
    private static final String TEST_DATE_TIME = TEST_DATE_TIME_FORMATTER.format(TEST_DATE.toInstant());

    // Verify round-trip consistency for both marshal-then-unmarshal and unmarshal-then-marshal operations.
    @Test
    public void testMarshalThenUnmarshal() throws Exception {
        assertEquals(TEST_DATE, DEFAULT_TIME_ZONE_ADAPTER.unmarshal(DEFAULT_TIME_ZONE_ADAPTER.marshal(TEST_DATE)));
    }

    @Test
    public void testUnmarshalThenMarshal() throws Exception {
        assertEquals(TEST_DATE_TIME, DEFAULT_TIME_ZONE_ADAPTER.marshal(DEFAULT_TIME_ZONE_ADAPTER.unmarshal(TEST_DATE_TIME)));
    }

    // Verify behavior after switching the JVM default time zone before invoking the adapter.
    @Test
    public void testMarshalAcrossTimeZones() throws Exception {
        assertAll(
                () -> assertMarshalInTimeZone("UTC", "01/02/2026 03:04:56 UTC"),
                () -> assertMarshalInTimeZone("Asia/Shanghai", "01/02/2026 11:04:56 CST"),
                () -> assertMarshalInTimeZone("America/Chicago", "01/01/2026 21:04:56 CST"),
                () -> assertMarshalInTimeZone("Asia/Kolkata", "01/02/2026 08:34:56 IST"),
                () -> assertMarshalInTimeZone("America/St_Johns", "01/01/2026 23:34:56 NST"),
                () -> assertMarshalInTimeZone("America/New_York", "01/01/2026 22:04:56 EST"),
                () -> assertMarshalInTimeZone("Asia/Tokyo", "01/02/2026 12:04:56 JST"),
                () -> assertMarshalInTimeZone("Australia/Adelaide", "01/02/2026 13:34:56 ACDT"),
                () -> assertMarshalInTimeZone("Pacific/Auckland", "01/02/2026 16:04:56 NZDT"),
                () -> assertMarshalInTimeZone("Europe/Berlin", "01/02/2026 04:04:56 CET")
        );
    }

    @Test
    public void testUnmarshalAcrossTimeZones() throws Exception {
        assertAll(
                () -> assertUnmarshalInTimeZone("UTC", "01/02/2026 03:04:56 UTC"),
                () -> assertUnmarshalInTimeZone("Asia/Shanghai", "01/02/2026 11:04:56 CST"),
                () -> assertUnmarshalInTimeZone("America/Chicago", "01/01/2026 21:04:56 CST"),
                () -> assertUnmarshalInTimeZone("Asia/Kolkata", "01/02/2026 08:34:56 IST"),
                () -> assertUnmarshalInTimeZone("America/St_Johns", "01/01/2026 23:34:56 NST"),
                () -> assertUnmarshalInTimeZone("America/New_York", "01/01/2026 22:04:56 EST"),
                () -> assertUnmarshalInTimeZone("Asia/Tokyo", "01/02/2026 12:04:56 JST"),
                () -> assertUnmarshalInTimeZone("Australia/Adelaide", "01/02/2026 13:34:56 ACDT"),
                () -> assertUnmarshalInTimeZone("Pacific/Auckland", "01/02/2026 16:04:56 NZDT"),
                () -> assertUnmarshalInTimeZone("Europe/Berlin", "01/02/2026 04:04:56 CET")
        );
    }

    private static void assertMarshalInTimeZone(final String timeZoneId, final String expectedDateTime) throws Exception {
        final TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZoneId));
            final DateTimeAdapter dateTimeAdapter = new DateTimeAdapter();
            assertEquals(expectedDateTime, dateTimeAdapter.marshal(TEST_DATE));
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    private static void assertUnmarshalInTimeZone(final String timeZoneId, final String dateTime) throws Exception {
        final TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZoneId));
            final DateTimeAdapter dateTimeAdapter = new DateTimeAdapter();
            assertEquals(TEST_DATE, dateTimeAdapter.unmarshal(dateTime));
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

}
