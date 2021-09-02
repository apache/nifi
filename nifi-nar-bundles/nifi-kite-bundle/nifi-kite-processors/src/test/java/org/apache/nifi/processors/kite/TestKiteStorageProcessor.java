/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.nifi.processors.kite.TestUtil.invalidStreamFor;
import static org.apache.nifi.processors.kite.TestUtil.streamFor;
import static org.apache.nifi.processors.kite.TestUtil.user;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS)
public class TestKiteStorageProcessor {

    private String datasetUri = null;
    private Dataset<Record> dataset = null;

    @BeforeEach
    public void createDataset() {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(TestUtil.USER_SCHEMA)
                .build();
        this.datasetUri = "dataset:file:" + new File(Files.createTempDir(), "ns/temp");
        this.dataset = Datasets.create(datasetUri, descriptor, Record.class);
    }

    @AfterEach
    public void deleteDataset() {
        Datasets.delete(datasetUri);
    }

    @Test
    public void testBasicStore() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.assertNotValid();

        runner.setProperty(StoreInKiteDataset.KITE_DATASET_URI, datasetUri);
        runner.assertValid();

        List<Record> users = Lists.newArrayList(
                user("a", "a@example.com"),
                user("b", "b@example.com"),
                user("c", "c@example.com")
        );

        runner.enqueue(streamFor(users));
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        runner.assertQueueEmpty();
        assertEquals(3, (long) runner.getCounterValue("Stored records"), "Should store 3 values");

        List<Record> stored = Lists.newArrayList(
                (Iterable<Record>) dataset.newReader());
        assertEquals(users, stored, "Records should match");
    }

    @Test
    public void testViewURI() {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(
                StoreInKiteDataset.KITE_DATASET_URI, "view:hive:ns/table?year=2015");
        runner.assertValid();
    }

    @Test
    public void testInvalidURI() {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(
                StoreInKiteDataset.KITE_DATASET_URI, "dataset:unknown");
        runner.assertNotValid();
    }

    @Test
    public void testUnreadableContent() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(StoreInKiteDataset.KITE_DATASET_URI, datasetUri);
        runner.assertValid();

        runner.enqueue(invalidStreamFor(user("a", "a@example.com")));
        runner.run();

        runner.assertAllFlowFilesTransferred("failure", 1);
    }

    @Test
    public void testCorruptedBlocks() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(StoreInKiteDataset.KITE_DATASET_URI, datasetUri);
        runner.assertValid();

        List<Record> records = Lists.newArrayList();
        for (int i = 0; i < 10000; i += 1) {
            String num = String.valueOf(i);
            records.add(user(num, num + "@example.com"));
        }

        runner.enqueue(invalidStreamFor(records));
        runner.run();

        long stored = runner.getCounterValue("Stored records");
        assertTrue(0 < stored && stored < 10000, "Should store some readable values");

        runner.assertAllFlowFilesTransferred("success", 1);
    }

    @Test
    public void testIncompatibleSchema() throws IOException {
        Schema incompatible = SchemaBuilder.record("User").fields()
                .requiredLong("id")
                .requiredString("username")
                .optionalString("email") // the dataset requires this field
                .endRecord();

        // this user has the email field and could be stored, but the schema is
        // still incompatible so the entire stream is rejected
        Record incompatibleUser = new Record(incompatible);
        incompatibleUser.put("id", 1L);
        incompatibleUser.put("username", "a");
        incompatibleUser.put("email", "a@example.com");

        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(StoreInKiteDataset.KITE_DATASET_URI, datasetUri);
        runner.assertValid();

        runner.enqueue(streamFor(incompatibleUser));
        runner.run();

        runner.assertAllFlowFilesTransferred("incompatible", 1);
    }
}
