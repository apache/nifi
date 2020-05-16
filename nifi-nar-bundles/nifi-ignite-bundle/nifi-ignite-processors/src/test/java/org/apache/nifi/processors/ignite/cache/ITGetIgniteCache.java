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

/**
 * Integration tests for {@link GetIgniteCache}.
 */
public class ITGetIgniteCache {

    private static final String CACHE_NAME = "testCache";

    private static TestRunner runner;
    private static GetIgniteCache getIgniteCache;
    private static Map<String, String> properties1;
    private static Map<String, String> properties2;

    @BeforeClass
    public static void setUp() {
        getIgniteCache = new GetIgniteCache();
        properties1 = new HashMap<>();
        properties2 = new HashMap<>();
    }

    @AfterClass
    public static void teardown() {
        runner = null;
        if (getIgniteCache.getThickIgniteClientCache() != null) {
            getIgniteCache.getThickIgniteClientCache().destroy();
        } else if (getIgniteCache.getThinIgniteClient() != null) {
            getIgniteCache.getThinIgniteClient().destroyCache(CACHE_NAME);
        }
        getIgniteCache = null;
    }

    private static void assertOneFlowFileRetrievedConfigurationProvided(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CONFIGURATION_FILE, "file:///" + new File(".").getAbsolutePath() + "/src/test/resources/test-ignite-client.xml");
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key5");
        runner.enqueue("test5".getBytes(), properties1);

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key5", "test5".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key5", "test5".getBytes());
        }
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out.assertContentEquals("test5".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test5".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key5"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test5".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key5"));
        }

        runner.shutdown();
    }

    private static void assertTwoFlowFilesRetrievedNoConfigurationProvided(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);
        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key51", "test51".getBytes());
            getIgniteCache.getThickIgniteClientCache().put("key52", "test52".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key51", "test51".getBytes());
            getIgniteCache.getThinIgniteClientCache().put("key52", "test52".getBytes());
        }
        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out.assertContentEquals("test51".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test51".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key51"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test51".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key51"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);
        out2.assertContentEquals("test52".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test52".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key52"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test52".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key52"));
        }

        runner.shutdown();
    }

    private static void assertTwoFlowFilesRetrievedAfterRestartingTwiceNoConfigurationProvided(final ClientType clientType) {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);
        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key51", "test51".getBytes());
            getIgniteCache.getThickIgniteClientCache().put("key52", "test52".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key51", "test51".getBytes());
            getIgniteCache.getThinIgniteClientCache().put("key52", "test52".getBytes());
        }
        runner.run(2, false, true);
        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        // Reinitialise and check first time.
        getIgniteCache.closeIgniteClientCache();
        runner.clearTransferState();
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);
        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        runner.run(2, false, true);
        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        // Reinitialise and check second time.
        getIgniteCache.closeIgniteClientCache();
        runner.clearTransferState();
        runner.assertValid();
        properties1.put("igniteKey", "key51");
        runner.enqueue("test1".getBytes(), properties1);
        properties2.put("igniteKey", "key52");
        runner.enqueue("test2".getBytes(), properties2);
        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        runner.run(2, false, true);
        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        runner.shutdown();
    }

    @Test
    public void getIgniteCache_configurationProvided_oneFlowFileRetrievedUsingThickIgniteClient() throws IOException {
        assertOneFlowFileRetrievedConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_configurationProvided_oneFlowFileRetrievedUsingThinIgniteClient() throws IOException {
        assertOneFlowFileRetrievedConfigurationProvided(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_noConfigurationProvided_twoFlowFilesRetrievedUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesRetrievedNoConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_noConfigurationProvided_twoFlowFilesRetrievedUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesRetrievedNoConfigurationProvided(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_noConfigurationProvided_twoFlowFilesRetrievedAfterRestartingTwiceUsingThickClient() {
        assertTwoFlowFilesRetrievedAfterRestartingTwiceNoConfigurationProvided(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_noConfigurationProvided_twoFlowFilesRetrievedAfterRestartingTwiceUsingThinClient() {
        assertTwoFlowFilesRetrievedAfterRestartingTwiceNoConfigurationProvided(ClientType.THIN);
    }
}
