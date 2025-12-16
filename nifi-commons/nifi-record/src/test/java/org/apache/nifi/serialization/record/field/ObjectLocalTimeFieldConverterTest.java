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
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectLocalTimeFieldConverterTest {

    private static final ObjectLocalTimeFieldConverter CONVERTER = new ObjectLocalTimeFieldConverter();

    private static final String FIELD_NAME = LocalTime.class.getSimpleName();

    private static final String TIME_DEFAULT = "12:30:45";

    private static final String TIME_WITH_OFFSET_PATTERN = "HH:mm:ssXXX";

    @ParameterizedTest
    @MethodSource("org.apache.nifi.serialization.record.field.DateTimeTestUtil#offsetSource")
    void testConvertFieldStringWithTimeOffset(final String offsetId) {
        final ZoneOffset zoneOffset = ZoneOffset.of(offsetId);

        final LocalTime localTime = LocalTime.parse(TIME_DEFAULT);
        final ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDate.EPOCH, localTime, zoneOffset);
        final ZonedDateTime zonedDateTimeAdjusted = zonedDateTime.withZoneSameInstant(ZoneId.systemDefault());
        final LocalTime expectedLocalTime = zonedDateTimeAdjusted.toLocalTime();

        final String localTimeWithOffsetId = TIME_DEFAULT + offsetId;
        final LocalTime convertedLocalTime = CONVERTER.convertField(localTimeWithOffsetId, Optional.of(TIME_WITH_OFFSET_PATTERN), FIELD_NAME);

        assertEquals(expectedLocalTime, convertedLocalTime);
    }
}
