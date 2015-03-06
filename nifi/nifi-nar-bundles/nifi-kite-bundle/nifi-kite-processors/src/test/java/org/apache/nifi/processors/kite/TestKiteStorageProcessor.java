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

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;

import static org.apache.nifi.processors.kite.TestUtil.invalidStreamFor;
import static org.apache.nifi.processors.kite.TestUtil.streamFor;
import static org.apache.nifi.processors.kite.TestUtil.user;

@Ignore("Does not work on windows")
public class TestKiteStorageProcessor {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String datasetUri = null;
  private Dataset<Record> dataset = null;

  @Before
  public void createDataset() throws Exception {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(TestUtil.USER_SCHEMA)
        .build();
    this.datasetUri = "dataset:file:" + temp.newFolder("ns", "temp").toString();
    this.dataset = Datasets.create(datasetUri, descriptor, Record.class);
  }

  @After
  public void deleteDataset() throws Exception {
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
    Assert.assertEquals("Should store 3 values",
        3, (long) runner.getCounterValue("Stored records"));

    List<Record> stored = Lists.newArrayList(
        (Iterable<Record>) dataset.newReader());
    Assert.assertEquals("Records should match", users, stored);
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
    Assert.assertTrue("Should store some readable values",
        0 < stored && stored < 10000);

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
