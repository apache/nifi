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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateTimeAdapterTest {

    private static final long TEST_DATE_TIME_MILLISECONDS = 1767323096000L; // 2026-01-02T03:04:56Z
    private static final Date TEST_DATE = new Date(TEST_DATE_TIME_MILLISECONDS);

    @ParameterizedTest
    @CsvSource({
            "UTC,                01/02/2026 03:04:56 UTC",
            "Asia/Shanghai,      01/02/2026 11:04:56 CST",
            "America/Chicago,    01/01/2026 21:04:56 CST",
            "Asia/Kolkata,       01/02/2026 08:34:56 IST",
            "America/St_Johns,   01/01/2026 23:34:56 NST",
            "America/New_York,   01/01/2026 22:04:56 EST",
            "Asia/Tokyo,         01/02/2026 12:04:56 JST",
            "Australia/Adelaide, 01/02/2026 13:34:56 ACDT",
            "Pacific/Auckland,   01/02/2026 16:04:56 NZDT",
            "Europe/Berlin,      01/02/2026 04:04:56 CET"
    })
    void testMarshalAndUnmarshal(final String timeZoneId, final String expectedDateTime) throws Exception {
        final TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZoneId));

            final DateTimeAdapter dateTimeAdapter = new DateTimeAdapter();

            assertEquals(expectedDateTime, dateTimeAdapter.marshal(TEST_DATE));

            assertEquals(TEST_DATE, dateTimeAdapter.unmarshal(expectedDateTime));
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }
}
