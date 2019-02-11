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

package org.apache.nifi.processors.mongodb.gridfs;

import com.mongodb.client.MongoCollection;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PutGridFSIT extends GridFSITTestBase {
    TestRunner runner;

    static final String BUCKET = "put_test_bucket";

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutGridFS.class);
        runner.setProperty(PutGridFS.FILE_NAME, String.format("${%s}", CoreAttributes.FILENAME.key()));
        super.setup(runner, BUCKET);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testSimplePut() {
        final String fileName = "simple_test.txt";
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);

        runner.enqueue("12345", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutGridFS.REL_SUCCESS);

        Assert.assertTrue("File does not exist", fileExists(fileName, BUCKET));
    }

    @Test
    public void testWithProperties() {
        final String fileName = "simple_test_props.txt";
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);
        attrs.put("prop.created_by", "john.smith");
        attrs.put("prop.created_for", "jane.doe");
        attrs.put("prop.restrictions", "PHI&PII");
        attrs.put("prop.department", "Accounting");

        runner.setProperty(PutGridFS.PROPERTIES_PREFIX, "prop");
        runner.enqueue("12345", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutGridFS.REL_SUCCESS);

        attrs = new HashMap<String, String>(){{
            put("created_by", "john.smith");
            put("created_for", "jane.doe");
            put("restrictions", "PHI&PII");
            put("department", "Accounting");
        }};

        Assert.assertTrue("File does not exist", fileExists(fileName, BUCKET));
        Assert.assertTrue("File is missing PARENT_PROPERTIES", fileHasProperties(fileName, BUCKET, attrs));
    }

    @Test
    public void testNoUniqueness() {
        String fileName = "test_duplicates.txt";
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);

        for (int x = 0; x < 10; x++) {
            runner.enqueue("Duplicates are ok.", attrs);
            runner.run();
        }

        runner.assertTransferCount(PutGridFS.REL_SUCCESS, 10);

        String bucketName = String.format("%s.files", BUCKET);
        MongoCollection files = client.getDatabase(DB).getCollection(bucketName);
        Document query = Document.parse(String.format("{\"filename\": \"%s\"}", fileName));
        long count = files.count(query);
        Assert.assertTrue("Wrong count", count == 10);
    }

    @Test
    public void testFileNameUniqueness() {
        String fileName = "test_duplicates.txt";
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);
        testUniqueness(attrs, "Hello, world", PutGridFS.UNIQUE_NAME);
    }

    @Test
    public void testFileNameAndHashUniqueness() {
        testHashUniqueness(PutGridFS.UNIQUE_BOTH);
    }

    @Test
    public void testHashUniqueness() {
        testHashUniqueness(PutGridFS.UNIQUE_HASH);
    }


    @Test
    public void testChunkSize() {
        String[] chunkSizes = new String[] { "128 KB", "256 KB", "384 KB", "512KB", "768KB", "1024 KB" };
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < 10000; x++) {
            sb.append("This is a test string used to build up a largish text file.");
        }
        final String testData = sb.toString();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), "big-putgridfs-test-file.txt");

        for (String chunkSize : chunkSizes) {
            runner.setProperty(PutGridFS.CHUNK_SIZE, chunkSize);
            runner.enqueue(testData, attrs);
            runner.run();
            runner.assertTransferCount(PutGridFS.REL_FAILURE, 0);
            runner.assertTransferCount(PutGridFS.REL_DUPLICATE, 0);
            runner.assertTransferCount(PutGridFS.REL_SUCCESS, 1);

            runner.clearTransferState();
        }

        runner.setProperty(PutGridFS.CHUNK_SIZE, "${gridfs.chunk.size}");
        attrs.put("gridfs.chunk.size", "768 KB");
        runner.enqueue(testData, attrs);
        runner.run();
        runner.assertTransferCount(PutGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(PutGridFS.REL_DUPLICATE, 0);
        runner.assertTransferCount(PutGridFS.REL_SUCCESS, 1);
    }

    private void testHashUniqueness(AllowableValue value) {
        String hashAttr = "hash.value";
        String fileName = "test_duplicates.txt";
        String content  = "Hello, world";
        String hash     = DigestUtils.md5Hex(content);
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAttributes.FILENAME.key(), fileName);
        attrs.put(hashAttr, hash);
        testUniqueness(attrs, content, value);
    }

    private void testUniqueness(Map<String, String> attrs, String content, AllowableValue param) {
        runner.setProperty(PutGridFS.ENFORCE_UNIQUENESS, param);
        for (int x = 0; x < 5; x++) {
            runner.enqueue(content, attrs);
            runner.run();
        }

        runner.assertTransferCount(PutGridFS.REL_FAILURE, 0);
        runner.assertTransferCount(PutGridFS.REL_DUPLICATE, 4);
        runner.assertTransferCount(PutGridFS.REL_SUCCESS, 1);
    }
}
