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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteCache;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITGetIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private static TestRunner runner;
    private static GetIgniteCache getIgniteCache;
    private static Map<String,String> properties1;
    private static HashMap<String, String> properties2;

    @BeforeClass
    public static void setUp() throws IOException {
        getIgniteCache = new GetIgniteCache();
        properties1 = new HashMap<String,String>();
        properties2 = new HashMap<String,String>();
    }

    @AfterClass
    public static void teardown() {
        runner = null;
        IgniteCache<String, byte[]> cache = getIgniteCache.getIgniteCache();
        if (cache != null )
            cache.destroy();
        getIgniteCache = null;
    }

    @Test
    public void testgetIgniteCacheOnTriggerFileConfigurationOneFlowFile() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CONFIGURATION_FILE,
                "file:///" + new File(".").getAbsolutePath() + "/src/test/resources/test-ignite.xml");
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key5");
        runner.enqueue("test5".getBytes(),properties1);

        getIgniteCache.initialize(runner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key5", "test5".getBytes());
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> sucessfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, sucessfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out.assertContentEquals("test5".getBytes());
        Assert.assertArrayEquals("test5".getBytes(),(byte[])getIgniteCache.getIgniteCache().get("key5"));
        runner.shutdown();
    }

    @Test
    public void testgetIgniteCacheOnTriggerNoConfigurationTwoFlowFile() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        getIgniteCache.initialize(runner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key51", "test51".getBytes());
        getIgniteCache.getIgniteCache().put("key52", "test52".getBytes());
        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);
        List<MockFlowFile> sucessfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, sucessfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out.assertContentEquals("test51".getBytes());
        Assert.assertArrayEquals("test51".getBytes(),
                (byte[])getIgniteCache.getIgniteCache().get("key51"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);

        out2.assertContentEquals("test52".getBytes());
        Assert.assertArrayEquals("test52".getBytes(),
                (byte[])getIgniteCache.getIgniteCache().get("key52"));

        runner.shutdown();
    }

    @Test
    public void testgetIgniteCacheOnTriggerNoConfigurationTwoFlowFileStopStart2Times() throws IOException, InterruptedException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        getIgniteCache.initialize(runner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key51", "test51".getBytes());
        getIgniteCache.getIgniteCache().put("key52", "test52".getBytes());
        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        getIgniteCache.closeIgniteCache();

        runner.clearTransferState();

        // reinit and check first time
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        getIgniteCache.initialize(runner.getProcessContext());

        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        getIgniteCache.closeIgniteCache();

        runner.clearTransferState();

        // reinit and check second time
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(),properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(),properties2);
        getIgniteCache.initialize(runner.getProcessContext());

        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        getIgniteCache.closeIgniteCache();

        runner.clearTransferState();

    }
}
