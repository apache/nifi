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

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestObjectLocalDateFieldConverter {
    private static final ObjectLocalDateFieldConverter CONVERTER = new ObjectLocalDateFieldConverter();

    @Test
    void testDefaultFormatterIterationFallsBackToNumericParse() {
        // Covers the default formatter loop in ObjectLocalDateFieldConverter.convertField which now uses 'continue'
        // (see ObjectLocalDateFieldConverter.java:84) to keep iterating before attempting the numeric fallback.
        assertThrows(FieldConversionException.class,
                () -> CONVERTER.convertField("not-a-date", Optional.empty(), "date"));
    }
}
