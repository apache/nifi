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
package org.apache.nifi.web.api.streaming;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardByteRangeParserTest {
    private static final String EMPTY = "";

    private static final String INVALID_UNIT = "octets=0-1";

    private static final String BYTES_RANGE_FORMAT = "bytes=%d-%d";

    private static final String BYTES_RANGE_LAST_POSITION_EMPTY = "bytes=%d-";

    private static final String BYTES_RANGE_FIRST_POSITION_EMPTY = "bytes=-%d";

    private static final String BYTES_RANGE_INVALID_NUMBER = "bytes=0-9988776655443322110";

    private static final String BYTES_RANGE_MISSING_NUMBERS = "bytes=-";

    private final StandardByteRangeParser parser = new StandardByteRangeParser();

    @Test
    void testReadByteRangeNull() {
        final Optional<ByteRange> byteRangeFound = parser.readByteRange(null);

        assertTrue(byteRangeFound.isEmpty());
    }

    @Test
    void testReadByteRangeEmpty() {
        final Optional<ByteRange> byteRangeFound = parser.readByteRange(EMPTY);

        assertTrue(byteRangeFound.isEmpty());
    }

    @Test
    void testReadByteRangeUnitNotValid() {
        assertThrows(ByteRangeFormatException.class, () -> parser.readByteRange(INVALID_UNIT));
    }

    @Test
    void testReadByteRangeNumbersNotSpecified() {
        assertThrows(ByteRangeFormatException.class, () -> parser.readByteRange(BYTES_RANGE_MISSING_NUMBERS));
    }

    @Test
    void testReadByteRangeNumberNotValid() {
        assertThrows(ByteRangeFormatException.class, () -> parser.readByteRange(BYTES_RANGE_INVALID_NUMBER));
    }

    @Test
    void testReadByteRangeLastPositionLessThanFirstPosition() {
        final String rangeHeader = BYTES_RANGE_FORMAT.formatted(Long.MAX_VALUE, 0);

        assertThrows(ByteRangeFormatException.class, () -> parser.readByteRange(rangeHeader));
    }

    @Test
    void testReadByteRangeZeroToUnspecified() {
        final long firstPosition = 0;

        final String rangeHeader = BYTES_RANGE_LAST_POSITION_EMPTY.formatted(firstPosition);

        final Optional<ByteRange> byteRangeFound = parser.readByteRange(rangeHeader);

        assertTrue(byteRangeFound.isPresent());

        final ByteRange byteRange = byteRangeFound.get();

        final OptionalLong firstPositionFound = byteRange.getFirstPosition();
        assertTrue(firstPositionFound.isPresent());
        assertEquals(firstPosition, firstPositionFound.getAsLong());

        final OptionalLong lastPositionFound = byteRange.getLastPosition();
        assertTrue(lastPositionFound.isEmpty());
    }

    @Test
    void testReadByteRangeSuffixRangeOne() {
        final long lastPosition = 1;

        final String rangeHeader = BYTES_RANGE_FIRST_POSITION_EMPTY.formatted(lastPosition);

        final Optional<ByteRange> byteRangeFound = parser.readByteRange(rangeHeader);

        assertTrue(byteRangeFound.isPresent());

        final ByteRange byteRange = byteRangeFound.get();

        final OptionalLong firstPositionFound = byteRange.getFirstPosition();
        assertTrue(firstPositionFound.isEmpty());

        final OptionalLong lastPositionFound = byteRange.getLastPosition();
        assertTrue(lastPositionFound.isPresent());
        assertEquals(lastPosition, lastPositionFound.getAsLong());
    }

    @Test
    void testReadByteRangeZeroToMaximumLong() {
        final long firstPosition = 0;
        final long lastPosition = Long.MAX_VALUE;

        final String rangeHeader = BYTES_RANGE_FORMAT.formatted(firstPosition, lastPosition);

        final Optional<ByteRange> byteRangeFound = parser.readByteRange(rangeHeader);

        assertTrue(byteRangeFound.isPresent());

        final ByteRange byteRange = byteRangeFound.get();

        final OptionalLong firstPositionFound = byteRange.getFirstPosition();
        assertTrue(firstPositionFound.isPresent());
        assertEquals(firstPosition, firstPositionFound.getAsLong());

        final OptionalLong lastPositionFound = byteRange.getLastPosition();
        assertTrue(lastPositionFound.isPresent());
        assertEquals(lastPosition, lastPositionFound.getAsLong());
    }
}
