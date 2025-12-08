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
package org.apache.nifi.serialization.record.field;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectLocalTimeFieldConverterTest {

    private static final ObjectLocalTimeFieldConverter CONVERTER = new ObjectLocalTimeFieldConverter();

    private static final String FIELD_NAME = LocalTime.class.getSimpleName();

    private static final String TIME_DEFAULT = "12:30:45";

    private static final String TIME_OFFSET = "Z";

    private static final String TIME_WITH_OFFSET = TIME_DEFAULT + TIME_OFFSET;

    private static final String TIME_WITH_OFFSET_PATTERN = "HH:mm:ssX";

    @ParameterizedTest
    @ValueSource(strings = {"GMT-03:00", "GMT", "GMT+03:00"})
    public void testConvertFieldStringWithTimeOffset(final String localTimeZone) {
        final LocalTime expected = ZonedDateTime.of(LocalDate.EPOCH, LocalTime.parse(TIME_WITH_OFFSET, DateTimeFormatter.ofPattern(TIME_WITH_OFFSET_PATTERN)), ZoneId.of(TIME_OFFSET))
                .withZoneSameInstant(ZoneId.of(localTimeZone))
                .toLocalTime();

        final TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(localTimeZone)));

            final LocalTime localTime = CONVERTER.convertField(TIME_WITH_OFFSET, Optional.of(TIME_WITH_OFFSET_PATTERN), FIELD_NAME);

            assertEquals(expected, localTime);
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }
}
