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
package org.apache.nifi.service.cassandra;

import org.apache.nifi.service.cql.api.lookup.CqlCell;
import org.apache.nifi.service.cql.api.lookup.CqlRow;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlCell;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlRow;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlStatementResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the schema-free row and cell types: the behaviour a caller relies on that is not simply the record
 * components handed back.
 */
class CqlRowAndCellTest {

    private static byte[] utf8(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // ---- Cell -----------------------------------------------------------------------------------------

    @Test
    @DisplayName("A ByteBuffer value is copied out as bytes without consuming the buffer the backend handed over")
    void testByteBufferIsReadWithoutConsumingIt() {
        final ByteBuffer shared = ByteBuffer.wrap(utf8("hello"));
        final CqlCell cell = new StandardCqlCell("v", shared);

        assertArrayEquals(utf8("hello"), cell.getBytes());
        assertEquals(5, shared.remaining(), "reading the cell must not advance the backend's buffer");
        assertArrayEquals(utf8("hello"), cell.getBytes(), "a second read must return the same bytes");
    }

    @Test
    @DisplayName("A byte[] value is copied, so a caller mutating the result cannot corrupt the cell")
    void testByteArrayIsCopied() {
        final byte[] original = utf8("hello");
        final CqlCell cell = new StandardCqlCell("v", original);

        final byte[] returned = cell.getBytes();
        returned[0] = 'J';

        assertArrayEquals(utf8("hello"), cell.getBytes());
    }

    @Test
    @DisplayName("A null cell reports null rather than an empty array, so absent and empty stay distinguishable")
    void testNullValue() {
        final CqlCell cell = new StandardCqlCell("v", null);

        assertTrue(cell.isNull());
        assertNull(cell.getBytes());
        assertNull(cell.getObject());
    }

    @Test
    @DisplayName("Asking a non-binary cell for bytes fails rather than inventing an encoding")
    void testGetBytesOnNonBinaryColumn() {
        final CqlCell cell = new StandardCqlCell("n", 42);

        final UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class, cell::getBytes);
        assertTrue(thrown.getMessage().contains("n"), thrown.getMessage());
    }

    @Test
    @DisplayName("A column whose CQL type has no JDK form throws on read, naming the type and the alternative")
    void testUnsupportedColumnType() {
        final CqlCell cell = StandardCqlCell.unsupported("address", "address_type");

        assertFalse(cell.isNull(), "an unreadable cell is not the same as a null one");
        final UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class, cell::getObject);
        assertTrue(thrown.getMessage().contains("address_type"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("query"), "should point at the API that can read it: " + thrown.getMessage());
    }

    // ---- Row ------------------------------------------------------------------------------------------

    private static CqlRow row() {
        return new StandardCqlRow(List.of(
                new StandardCqlCell("k", ByteBuffer.wrap(utf8("key"))),
                new StandardCqlCell("writetime(v)", 1234L),
                new StandardCqlCell("v", ByteBuffer.wrap(utf8("value")))));
    }

    @Test
    @DisplayName("Cells come back in selection order, which is the only ordering information a schema-free caller has")
    void testCellsPreserveSelectionOrder() {
        assertEquals(List.of("k", "writetime(v)", "v"), row().columnNames());
    }

    @Test
    @DisplayName("A column name that is not a legal CQL identifier is carried through unchanged")
    void testIllegalIdentifierColumnNameIsPreserved() {
        assertEquals(1234L, row().getObject("writetime(v)"));
    }

    @Test
    @DisplayName("Looking up a column that is not in the row fails and says what the row does have")
    void testMissingColumnThrows() {
        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> row().getBytes("nope"));

        assertTrue(thrown.getMessage().contains("nope"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("writetime(v)"), "should list the actual columns: " + thrown.getMessage());
    }

    @Test
    @DisplayName("A duplicated column name keeps both cells, which is why a row is a list rather than a map")
    void testDuplicateColumnNamesAreBothRetained() {
        final CqlRow duplicated = new StandardCqlRow(List.of(
                new StandardCqlCell("v", ByteBuffer.wrap(utf8("first"))),
                new StandardCqlCell("v", ByteBuffer.wrap(utf8("second")))));

        assertEquals(2, duplicated.cells().size());
        assertEquals(List.of("v", "v"), duplicated.columnNames());
        // The by-name shorthand can only answer with one of them, which is precisely why cells() exists.
        assertArrayEquals(utf8("first"), duplicated.getBytes("v"));
        assertArrayEquals(utf8("second"), duplicated.cells().get(1).getBytes());
    }

    @Test
    @DisplayName("findCell distinguishes an absent column from a present one holding null")
    void testFindCellSeparatesAbsentFromNull() {
        final CqlRow withNull = new StandardCqlRow(List.of(new StandardCqlCell("v", null)));

        assertTrue(withNull.findCell("v").isPresent(), "the column is present");
        assertNull(withNull.getBytes("v"), "...and holds a null");
        assertTrue(withNull.findCell("absent").isEmpty());
    }

    @Test
    @DisplayName("A row's cells cannot be changed through the list handed to it or the list handed back")
    void testCellsAreImmutable() {
        assertThrows(UnsupportedOperationException.class, () -> row().cells().clear());
    }

    // ---- Result ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("An applied conditional write reports true and carries no row, since there was nothing to return")
    void testAppliedResult() {
        final StandardCqlStatementResult result = new StandardCqlStatementResult(true, List.of());

        assertTrue(result.wasApplied());
        assertTrue(result.rows().isEmpty());
    }

    @Test
    @DisplayName("A rejected conditional write reports false and carries the row that is actually stored")
    void testRejectedResultCarriesCurrentRow() {
        final StandardCqlStatementResult result = new StandardCqlStatementResult(false, List.of(row()));

        assertFalse(result.wasApplied());
        assertArrayEquals(utf8("value"), result.rows().getFirst().getBytes("v"));
    }
}
