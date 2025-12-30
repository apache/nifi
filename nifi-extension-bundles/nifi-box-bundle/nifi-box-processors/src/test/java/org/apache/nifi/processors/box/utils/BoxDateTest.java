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
package org.apache.nifi.processors.box.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoxDateTest {

    @ParameterizedTest
    @CsvSource({
            "2025-01-01, 2025-01-01T00:00:00.000Z",
            "2025-02-25, 2025-02-25T00:00:00.000Z",
            "2025-11-10, 2025-11-10T00:00:00.000Z",
    })
    void format(final LocalDate date, final String expected) {
        final BoxDate boxDate = BoxDate.of(date);
        final String formatted = boxDate.format();

        assertEquals(expected, formatted);
    }
}
