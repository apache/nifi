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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDeduplicateRecord {

    private TestRunner runner;
    private MockRecordParser reader;
    private MockRecordWriter writer;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(DeduplicateRecord.class);

        // RECORD_READER, RECORD_WRITER
        reader = new MockRecordParser();
        writer = new MockRecordWriter("header", false);

        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(DeduplicateRecord.RECORD_READER, "reader");
        runner.setProperty(DeduplicateRecord.RECORD_WRITER, "writer");
        runner.setProperty(DeduplicateRecord.RECORD_HASHING_ALGORITHM, DeduplicateRecord.SHA256_ALGORITHM_VALUE);

        reader.addSchemaField("firstName", RecordFieldType.STRING);
        reader.addSchemaField("middleName", RecordFieldType.STRING);
        reader.addSchemaField("lastName", RecordFieldType.STRING);

        // INCLUDE_ZERO_RECORD_FLOWFILES
        runner.setProperty(DeduplicateRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "true");

        runner.assertValid();
    }

    void commonEnqueue() {
        final Map<String, String> props = new HashMap<>();
        props.put("hash.value", "1000");
        runner.enqueue(new byte[]{}, props);
    }

    @Test
    public void testInvalidRecordPathCausesValidationError() {
        runner.setProperty(DeduplicateRecord.FILTER_TYPE, DeduplicateRecord.HASH_SET_VALUE);
        runner.setProperty("middle_name", "//////middleName");
        runner.assertNotValid();
    }

    @Test
    public void testDetectDuplicatesHashSet() {
        commonEnqueue();

        runner.setProperty(DeduplicateRecord.FILTER_TYPE, DeduplicateRecord.HASH_SET_VALUE);
        runner.setProperty("middle_name", "/middleName");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);
    }

    @Test
    public void testDetectDuplicatesBloomFilter() {
        commonEnqueue();
        runner.setProperty(DeduplicateRecord.FILTER_TYPE, DeduplicateRecord.BLOOM_FILTER_VALUE);
        runner.setProperty(DeduplicateRecord.BLOOM_FILTER_FPP, "0.10");
        runner.setProperty("middle_name", "/middleName");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);
    }

    @Test
    public void testNoDuplicatesHashSet() {
        commonEnqueue();
        runner.setProperty(DeduplicateRecord.FILTER_TYPE, DeduplicateRecord.HASH_SET_VALUE);
        runner.setProperty("middle_name", "/middleName");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testNoDuplicatesBloomFilter() {
        commonEnqueue();
        runner.setProperty(DeduplicateRecord.FILTER_TYPE, DeduplicateRecord.BLOOM_FILTER_VALUE);
        runner.setProperty(DeduplicateRecord.BLOOM_FILTER_FPP, "0.10");
        runner.setProperty("middle_name", "/middleName");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testAllDuplicates() {
        commonEnqueue();
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("John", "Q", "Smith");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 1, 2);
    }

    @Test
    public void testAllUnique() {
        commonEnqueue();
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testCacheValueFromRecordPath() {
        commonEnqueue();
        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 1);
    }

    /*
     * These are all related to NIFI-6014
     */

    @Test
    public void testMultipleFileDeduplicationRequiresDMC() {
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.assertNotValid();
    }

    public static final String FIRST_KEY = String.join(String.valueOf(DeduplicateRecord.JOIN_CHAR), Arrays.asList(
            "John", "Q", "Smith"
    ));
    public static final String SECOND_KEY = String.join(String.valueOf(DeduplicateRecord.JOIN_CHAR), Arrays.asList(
            "Jack", "Z", "Brown"
    ));
    public static final String FIRST_KEY_HASHED = DigestUtils.sha256Hex(FIRST_KEY);
    public static final String SECOND_KEY_HASHED = DigestUtils.sha256Hex(SECOND_KEY);

    @Test
    public void testDeduplicateWithDMCNoPutIdentifier() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.enableControllerService(dmc);
        runner.assertValid();

        dmc.put(FIRST_KEY_HASHED, true, null, null);

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 1);
    }

    @Test
    public void testDeduplicateWithDMCPutIdentifier() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.PUT_CACHE_IDENTIFIER, "true");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.enableControllerService(dmc);
        runner.assertValid();

        dmc.put(FIRST_KEY_HASHED, true, null, null);

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 2, 2);
    }

    @Test
    public void testDeduplicateWithDMCAndOneRecordPath() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.PUT_CACHE_IDENTIFIER, "true");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.setProperty("first_name", "/firstName");
        runner.enableControllerService(dmc);
        runner.assertValid();

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 1);
    }

    @Test
    public void testDeduplicateWithDMCAndMultipleRecordPaths() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.PUT_CACHE_IDENTIFIER, "true");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.setProperty("first_name", "/firstName");
        runner.setProperty("middle_name", "/middleName");
        runner.setProperty("invalid_property", "/missingProperty");
        runner.enableControllerService(dmc);
        runner.assertValid();

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 1);
    }

    @Test
    public void testDeduplicateWithDMCAndNullField() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.PUT_CACHE_IDENTIFIER, "true");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.setProperty("middle_name", "/middleName");
        runner.setProperty("last_name", "/lastName");
        runner.enableControllerService(dmc);
        runner.assertValid();

        reader.addRecord("Jack", "Z", null);
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "R", "Brown");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 3, 0);
    }

    @Test
    public void testDeduplicateNoHashing() throws Exception  {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.setProperty(DeduplicateRecord.RECORD_HASHING_ALGORITHM, DeduplicateRecord.NONE_ALGORITHM_VALUE);
        runner.enableControllerService(dmc);
        runner.assertValid();

        dmc.put(FIRST_KEY, true, null, null);
        dmc.put(SECOND_KEY, true, null, null);

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        runner.enqueue("");
        runner.run();

        doCountTests(0, 1, 1, 1, 1, 3);
    }

    @Test
    public void testDeduplicateWithDMCAndCacheIdentifier() throws Exception {
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(DeduplicateRecord.DISTRIBUTED_MAP_CACHE, "dmc");
        runner.setProperty(DeduplicateRecord.DEDUPLICATION_STRATEGY, DeduplicateRecord.OPTION_MULTIPLE_FILES.getValue());
        runner.setProperty(DeduplicateRecord.CACHE_IDENTIFIER, "${user.name}-${" + DeduplicateRecord.RECORD_HASH_VALUE_ATTRIBUTE + "}");
        runner.setProperty(DeduplicateRecord.PUT_CACHE_IDENTIFIER, "true");
        runner.enableControllerService(dmc);
        runner.assertValid();

        dmc.put(String.format("john.smith-%s", FIRST_KEY_HASHED), true, null, null);
        dmc.put(String.format("john.smith-%s", SECOND_KEY_HASHED), true, null, null);

        reader.addRecord("John", "Q", "Smith");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jack", "Z", "Brown");
        reader.addRecord("Jane", "X", "Doe");

        Map<String, String> attrs = new HashMap<>();
        attrs.put("user.name", "john.smith");

        runner.enqueue("", attrs);
        runner.run();

        doCountTests(0, 1, 1, 1, 1, 3);
    }

    void doCountTests(int failure, int original, int duplicates, int notDuplicates, int notDupeCount, int dupeCount) {
        runner.assertTransferCount(DeduplicateRecord.REL_FAILURE, failure);
        runner.assertTransferCount(DeduplicateRecord.REL_ORIGINAL, original);
        runner.assertTransferCount(DeduplicateRecord.REL_DUPLICATE, duplicates);
        runner.assertTransferCount(DeduplicateRecord.REL_NON_DUPLICATE, notDuplicates);

        List<MockFlowFile> duplicateFlowFile = runner.getFlowFilesForRelationship(DeduplicateRecord.REL_DUPLICATE);
        if (duplicateFlowFile != null) {
            assertEquals(String.valueOf(dupeCount), duplicateFlowFile.get(0).getAttribute(DeduplicateRecord.RECORD_COUNT_ATTRIBUTE));
        }

        List<MockFlowFile> nonDuplicateFlowFile = runner.getFlowFilesForRelationship(DeduplicateRecord.REL_NON_DUPLICATE);
        if (nonDuplicateFlowFile != null) {
            assertEquals(String.valueOf(notDupeCount), nonDuplicateFlowFile.get(0).getAttribute(DeduplicateRecord.RECORD_COUNT_ATTRIBUTE));
        }
    }

}
