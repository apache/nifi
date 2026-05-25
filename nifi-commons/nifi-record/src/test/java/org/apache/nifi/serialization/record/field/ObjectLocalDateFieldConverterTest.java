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

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ObjectLocalDateFieldConverterTest {

    private static final ObjectLocalDateFieldConverter CONVERTER = new ObjectLocalDateFieldConverter();
    private static final String FIELD_NAME = LocalDate.class.getSimpleName();

    @Test
    void testConvertFieldNull() {
        assertNull(CONVERTER.convertField(null, Optional.empty(), FIELD_NAME));
    }

    @Test
    void testConvertFieldLocalDate() {
        final LocalDate input = LocalDate.of(2025, 5, 25);
        assertEquals(input, CONVERTER.convertField(input, Optional.empty(), FIELD_NAME));
    }

    @Test
    void testConvertFieldSqlDateModernYear() {
        final LocalDate localDate = LocalDate.of(2025, 5, 25);
        final Date sqlDate = new Date(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
        final LocalDate result = CONVERTER.convertField(sqlDate, Optional.empty(), FIELD_NAME);
        assertEquals(localDate, result);
    }

    @Test
    void testConvertFieldSqlDateYearOneIsProlepticGregorian() {
        final LocalDate yearOne = LocalDate.of(1, 1, 1);
        final Date sqlDate = new Date(yearOne.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
        final LocalDate result = CONVERTER.convertField(sqlDate, Optional.empty(), FIELD_NAME);
        assertEquals(yearOne, result);
    }
}
