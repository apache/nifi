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
package org.apache.nifi.processors.ignite.cache;

import org.apache.nifi.processors.ignite.ClientType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ITPutIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private static TestRunner runner;
    private static PutIgniteCache putIgniteCache;
    private static Map<String, String> properties1;
    private static Map<String, String> properties2;

    @BeforeClass
    public static void setUp() {
        putIgniteCache = new PutIgniteCache();
        properties1 = new HashMap<>();
        properties2 = new HashMap<>();
    }

    @AfterClass
    public static void teardown() {
        runner = null;
        if (putIgniteCache.getThickIgniteClientCache() != null) {
            putIgniteCache.getThickIgniteClientCache().destroy();
        } else if (putIgniteCache.getThinIgniteClient() != null) {
            putIgniteCache.getThinIgniteClient().destroyCache(CACHE_NAME);
        }
        putIgniteCache = null;
    }

    private static void assertFlowFilePutConfigurationProvided(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.IGNITE_CONFIGURATION_FILE, "file:///" + new File(".").getAbsolutePath() + "/src/test/resources/test-ignite-client.xml");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key5");
        runner.enqueue("test".getBytes(), properties1);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertContentEquals("test".getBytes());

        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key5"));
            putIgniteCache.getThickIgniteClientCache().remove("key5");
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key5"));
            putIgniteCache.getThinIgniteClientCache().remove("key5");
        }

        runner.shutdown();
    }

    private static void assertTwoFlowFilesPutAfterRestartingTwiceNoConfigurationProvided(final ClientType clientType) {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);

        if (clientType.equals(ClientType.THICK)) {
            putIgniteCache.getThickIgniteClientCache().remove("key51");
            putIgniteCache.getThickIgniteClientCache().remove("key52");
        } else if (clientType.equals(ClientType.THIN)) {
            putIgniteCache.getThinIgniteClientCache().remove("key51");
            putIgniteCache.getThinIgniteClientCache().remove("key52");
        }

        // Reinitialise and check first time.
        if (clientType.equals(ClientType.THICK)) {
            putIgniteCache.closeThickIgniteClientDataStreamer();
        }
        putIgniteCache.closeIgniteClientCache();
        runner.clearTransferState();
        putIgniteCache.initializePutIgniteCacheProcessor(runner.getProcessContext());
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);

        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);

        if (clientType.equals(ClientType.THICK)) {
            putIgniteCache.getThickIgniteClientCache().remove("key51");
            putIgniteCache.getThickIgniteClientCache().remove("key52");
        } else if (clientType.equals(ClientType.THIN)) {
            putIgniteCache.getThinIgniteClientCache().remove("key51");
            putIgniteCache.getThinIgniteClientCache().remove("key52");
        }

        // Reinitialise and check second time.
        if (clientType.equals(ClientType.THICK)) {
            putIgniteCache.closeThickIgniteClientDataStreamer();
        }
        putIgniteCache.closeIgniteClientCache();
        runner.clearTransferState();
        putIgniteCache.initializePutIgniteCacheProcessor(runner.getProcessContext());
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);

        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);

        if (clientType.equals(ClientType.THICK)) {
            putIgniteCache.getThickIgniteClientCache().remove("key51");
            putIgniteCache.getThickIgniteClientCache().remove("key52");
        } else if (clientType.equals(ClientType.THIN)) {
            putIgniteCache.getThinIgniteClientCache().remove("key51");
            putIgniteCache.getThinIgniteClientCache().remove("key52");
        }

        runner.shutdown();
    }

    private static void assertTwoFlowFilesPutNoConfigurationProvided(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertContentEquals("test1".getBytes());

        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key51"));
            putIgniteCache.getThickIgniteClientCache().remove("key51");
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key51"));
            putIgniteCache.getThinIgniteClientCache().remove("key51");
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertContentEquals("test2".getBytes());

        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key52"));
            putIgniteCache.getThickIgniteClientCache().remove("key52");
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key52"));
            putIgniteCache.getThinIgniteClientCache().remove("key52");
        }

        runner.shutdown();
    }

    @Test
    public void putIgniteCache_configurationProvided_oneFlowFilePutUsingThickIgniteClient() throws IOException {
        assertFlowFilePutConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_configurationProvided_oneFlowFilePutUsingThinIgniteClient() throws IOException {
        assertFlowFilePutConfigurationProvided(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_noConfigurationProvided_twoFlowFilesPutUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesPutNoConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_noConfigurationProvided_twoFlowFilesPutUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesPutNoConfigurationProvided(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_noConfigurationProvided_twoFlowFilesPutAfterRestartingTwiceUsingThickClient() {
        assertTwoFlowFilesPutAfterRestartingTwiceNoConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_noConfigurationProvided_twoFlowFilesPutAfterRestartingTwiceUsingThinClient() {
        assertTwoFlowFilesPutAfterRestartingTwiceNoConfigurationProvided(ClientType.THIN);
    }
}
