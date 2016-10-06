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
    private static Map<String,String> properties1;
    private static HashMap<String, String> properties2;

    @BeforeClass
    public static void setUp() throws IOException {
        putIgniteCache = new PutIgniteCache();
        properties1 = new HashMap<String,String>();
        properties2 = new HashMap<String,String>();
    }

    @AfterClass
    public static void teardown() {
        runner = null;
        putIgniteCache.getIgniteCache().destroy();
        putIgniteCache = null;
    }

    @Test
    public void testPutIgniteCacheOnTriggerFileConfigurationOneFlowFile() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.IGNITE_CONFIGURATION_FILE,
                "file:///" + new File(".").getAbsolutePath() + "/src/test/resources/test-ignite.xml");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key5");
        runner.enqueue("test".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> sucessfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, sucessfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out.assertContentEquals("test".getBytes());
        System.out.println("Value was: " + new String((byte[])putIgniteCache.getIgniteCache().get("key5")));
        Assert.assertArrayEquals("test".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key5"));
        putIgniteCache.getIgniteCache().remove("key5");
    }

    @Test
    public void testPutIgniteCacheOnTriggerNoConfigurationTwoFlowFile() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        List<MockFlowFile> sucessfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, sucessfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out.assertContentEquals("test1".getBytes());
        System.out.println("value was " + new String(putIgniteCache.getIgniteCache().get("key51")));
        Assert.assertArrayEquals("test1".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key51"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");

        out2.assertContentEquals("test2".getBytes());
        Assert.assertArrayEquals("test2".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key52"));
        putIgniteCache.getIgniteCache().remove("key52");
        putIgniteCache.getIgniteCache().remove("key51");

    }

    @Test
    public void testPutIgniteCacheOnTriggerNoConfigurationTwoFlowFileStopAndStart2Times() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);
        putIgniteCache.getIgniteCache().remove("key51");
        putIgniteCache.getIgniteCache().remove("key52");

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        putIgniteCache.getIgniteCache().remove("key52");
        putIgniteCache.getIgniteCache().remove("key52");

        // Close and restart first time
        putIgniteCache.closeIgniteDataStreamer();

        runner.clearTransferState();

        putIgniteCache.initializeIgniteDataStreamer(runner.getProcessContext());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        putIgniteCache.getIgniteCache().remove("key51");
        putIgniteCache.getIgniteCache().remove("key52");

        // Close and restart second time
        putIgniteCache.closeIgniteDataStreamer();

        runner.clearTransferState();

        putIgniteCache.initializeIgniteDataStreamer(runner.getProcessContext());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        putIgniteCache.getIgniteCache().remove("key52");
        putIgniteCache.getIgniteCache().remove("key51");

    }
}
