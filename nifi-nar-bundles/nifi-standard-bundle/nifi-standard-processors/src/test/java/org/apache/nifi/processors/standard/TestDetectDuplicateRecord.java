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
package org.apache.nifi.processors.standard;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.standard.DetectDuplicateRecord.*;
import static org.junit.Assert.assertEquals;

public class TestDetectDuplicateRecord {

    private TestRunner runner;
    private MockCacheService cache;
    private MockRecordParser reader;
    private MockRecordWriter writer;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.DetectDuplicateRecord", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestDetectDuplicateRecord", "debug");
    }

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(DetectDuplicateRecord.class);

        // RECORD_READER, RECORD_WRITER
        reader = new MockRecordParser();
        writer = new MockRecordWriter("header", false);

        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(RECORD_READER, "reader");
        runner.setProperty(RECORD_WRITER, "writer");

        reader.addSchemaField("firstName", RecordFieldType.STRING);
        reader.addSchemaField("middleName", RecordFieldType.STRING);
        reader.addSchemaField("lastName", RecordFieldType.STRING);

        // INCLUDE_ZERO_RECORD_FLOWFILES
        runner.setProperty(INCLUDE_ZERO_RECORD_FLOWFILES, "true");

        // CACHE_IDENTIFIER
        runner.setProperty(CACHE_IDENTIFIER, "true");

        // DISTRIBUTED_CACHE_SERVICE
        cache = new MockCacheService();
        runner.addControllerService("cache", cache);
        runner.setProperty(DISTRIBUTED_CACHE_SERVICE, "cache");
        runner.enableControllerService(cache);

        // CACHE_ENTRY_IDENTIFIER
        final Map<String, String> props = new HashMap<>();
        props.put("hash.value", "1000");
        runner.enqueue(new byte[]{}, props);

        // AGE_OFF_DURATION
        runner.setProperty(AGE_OFF_DURATION, "48 hours");

        runner.assertValid();
    }

     @Test
     public void testDetectDuplicatesHashSet() {
        runner.setProperty(FILTER_TYPE, HASH_SET_VALUE);
        runner.setProperty("/middleName", "${field.value}");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);
    }

    @Test
    public void testDetectDuplicatesBloomFilter() {
        runner.setProperty(FILTER_TYPE, BLOOM_FILTER_VALUE);
        runner.setProperty(BLOOM_FILTER_FPP, "0.10");
        runner.setProperty("/middleName", "${field.value}");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);
    }

    @Test
    public void testNoDuplicatesHashSet() {
        runner.setProperty(FILTER_TYPE, HASH_SET_VALUE);
        runner.setProperty("/middleName", "${field.value}");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testNoDuplicatesBloomFilter() {
        runner.setProperty(FILTER_TYPE, BLOOM_FILTER_VALUE);
        runner.setProperty(BLOOM_FILTER_FPP, "0.10");
        runner.setProperty("/middleName", "${field.value}");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testAllDuplicates() {
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 0, 1, 2);
    }

    @Test
    public void testAllUnique() {
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }



    @Test
    public void testCacheValueFromRecordPath() {
        runner.setProperty(CACHE_ENTRY_IDENTIFIER, "Users");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);

        cache.assertContains("KEY", "VALUE"); // TODO: Get the tests running so you can see what the key/value is in serialized form
    }

    void doCountTests(int failure, int original, int duplicates, int notDuplicates, int notDupeCount, int dupeCount) {
        runner.assertTransferCount(REL_DUPLICATE, duplicates);
        runner.assertTransferCount(REL_NON_DUPLICATE, notDuplicates);
        runner.assertTransferCount(REL_ORIGINAL, original);
        runner.assertTransferCount(REL_FAILURE, failure);

        List<MockFlowFile> duplicateFlowFile = runner.getFlowFilesForRelationship(REL_DUPLICATE);
        if (duplicateFlowFile != null) {
            assertEquals(String.valueOf(dupeCount), duplicateFlowFile.get(0).getAttribute("record.count"));
        }

        List<MockFlowFile> nonDuplicateFlowFile = runner.getFlowFilesForRelationship(REL_NON_DUPLICATE);
        if (nonDuplicateFlowFile != null) {
            assertEquals(String.valueOf(notDupeCount), nonDuplicateFlowFile.get(0).getAttribute("record.count"));
        }
    }
}
