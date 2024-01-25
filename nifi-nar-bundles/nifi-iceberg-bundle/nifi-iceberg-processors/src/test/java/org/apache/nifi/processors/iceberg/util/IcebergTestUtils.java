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
package org.apache.nifi.processors.iceberg.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang.Validate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergTestUtils {

    /**
     * Validates whether the table contains the expected records. The results should be sorted by a unique key, so we do not end up with flaky tests.
     *
     * @param table    The table we should read the records from
     * @param expected The expected list of Records
     * @param sortBy   The column position by which we will sort
     * @throws IOException Exceptions when reading the table data
     */
    public static void validateData(Table table, List<Record> expected, int sortBy) throws IOException {
        List<Record> records = Lists.newArrayListWithExpectedSize(expected.size());
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            iterable.forEach(records::add);
        }

        validateData(expected, records, sortBy);
    }

    /**
     * Validates whether the 2 sets of records are the same. The results should be sorted by a unique key, so we do not end up with flaky tests.
     *
     * @param expected The expected list of Records
     * @param actual   The actual list of Records
     * @param sortBy   The column position by which we will sort
     */
    public static void validateData(List<Record> expected, List<Record> actual, int sortBy) {
        List<Record> sortedExpected = Lists.newArrayList(expected);
        List<Record> sortedActual = Lists.newArrayList(actual);
        // Sort based on the specified field
        sortedExpected.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));
        sortedActual.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));

        assertEquals(sortedExpected.size(), sortedActual.size());
        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(sortedExpected.get(i), sortedActual.get(i));
        }
    }

    /**
     * Validates the number of files under a {@link Table}
     *
     * @param tableLocation     The location of table we are checking
     * @param numberOfDataFiles The expected number of data files (TABLE_LOCATION/data/*)
     */
    public static void validateNumberOfDataFiles(String tableLocation, int numberOfDataFiles) throws IOException {
        List<Path> dataFiles = Files.walk(Paths.get(tableLocation + "/data"))
                .filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().startsWith("."))
                .toList();

        assertEquals(numberOfDataFiles, dataFiles.size());
    }

    public static void validatePartitionFolders(String tableLocation, List<String> partitionPaths) {
        for (String partitionPath : partitionPaths) {
            Path path = Paths.get(tableLocation + "/data/" + partitionPath);
            assertTrue(Files.exists(path),"The expected path doesn't exists: " + path);
        }
    }

    public static class RecordsBuilder {

        private final List<Record> records = new ArrayList<>();
        private final Schema schema;

        private RecordsBuilder(Schema schema) {
            this.schema = schema;
        }

        public RecordsBuilder add(Object... values) {
            Validate.isTrue(schema.columns().size() == values.length, "Number of provided values and schema length should be equal.");

            GenericRecord record = GenericRecord.create(schema);

            for (int i = 0; i < values.length; i++) {
                record.set(i, values[i]);
            }

            records.add(record);
            return this;
        }

        public List<Record> build() {
            return Collections.unmodifiableList(records);
        }

        public static RecordsBuilder newInstance(Schema schema) {
            return new RecordsBuilder(schema);
        }
    }

}
