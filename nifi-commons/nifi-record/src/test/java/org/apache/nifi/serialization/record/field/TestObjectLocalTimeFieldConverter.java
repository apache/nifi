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

import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestObjectLocalTimeFieldConverter {
    private static final ObjectLocalTimeFieldConverter CONVERTER = new ObjectLocalTimeFieldConverter();

    @Test
    void testIsoLocalTimeParsedWithoutPattern() {
        final LocalTime result = CONVERTER.convertField("13:45:30", Optional.empty(), "time");
        assertEquals(LocalTime.of(13, 45, 30), result);
    }

    @Test
    void testIsoLocalTimeWithMillisParsedWithoutPattern() {
        final LocalTime result = CONVERTER.convertField("13:45:30.123", Optional.empty(), "time");
        assertEquals(LocalTime.of(13, 45, 30, 123_000_000), result);
    }

    @Test
    void testInvalidStringFallsBackToNumericThenThrows() {
        assertThrows(FieldConversionException.class,
                () -> CONVERTER.convertField("not-a-time", Optional.empty(), "time"));
    }
}
