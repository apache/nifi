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
package org.apache.nifi.processors.standard.db.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPhoenixDatabaseAdapter {
    private PhoenixDatabaseAdapter testSubject;

    @BeforeEach
    public void setUp() throws Exception {
        testSubject = new PhoenixDatabaseAdapter();
    }

    @Test
    void testSupportsUpsert() {
        assertTrue(testSubject.supportsUpsert(), testSubject.getClass().getSimpleName() + " should support upsert");
    }

    @Test
    void testGetUpsertStatementWithNullTableName() {
        testGetUpsertStatement(null, Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    void testGetUpsertStatementWithBlankTableName() {
        testGetUpsertStatement("", Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    void testGetUpsertStatementWithNullColumnNames() {
        testGetUpsertStatement("notEmpty", null, Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    void testGetUpsertStatementWithEmptyColumnNames() {
        testGetUpsertStatement("notEmpty", Collections.emptyList(), Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    void testGetUpsertStatementWithNullKeyColumnNames() {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), null, new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    void testGetUpsertStatementWithEmptyKeyColumnNames() {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), Collections.emptyList(), new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    void testGetUpsertStatement() {
        // GIVEN
        String tableName = "table";
        List<String> columnNames = Arrays.asList("column1", "column2", "column3", "column4");
        Collection<String> uniqueKeyColumnNames = Arrays.asList("column2", "column4");

        String expected = "UPSERT INTO" +
                " table(column1, column2, column3, column4) VALUES (?, ?, ?, ?)";

        // WHEN
        // THEN
        testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, expected);
    }

    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, IllegalArgumentException expected) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, (String) null);
        });
        assertEquals(expected.getMessage(), e.getMessage());
    }

    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, String expected) {
        // WHEN
        String actual = testSubject.getUpsertStatement(tableName, columnNames, uniqueKeyColumnNames);

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testGetUpsertStatementQuoted() {
        // GIVEN
        String tableName = "\"table\"";
        List<String> columnNames = Arrays.asList("column1", "\"column2\"", "column3", "column4");
        Collection<String> uniqueKeyColumnNames = Arrays.asList("\"column2\"", "column4");

        String expected = "UPSERT INTO" +
                " \"table\"(column1, \"column2\", column3, column4) VALUES (?, ?, ?, ?)";

        // WHEN
        // THEN
        testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, expected);
    }
}
