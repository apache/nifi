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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.internal.IgnitionEx.loadConfiguration;
import static org.junit.Assert.assertEquals;

public class TestGetIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private static Ignite ignite;
    private TestRunner getRunner;
    private GetIgniteCache getIgniteCache;
    private Map<String, String> properties1;
    private Map<String, String> properties2;

    @BeforeClass
    public static void setUpClass() throws IgniteCheckedException {
        final String targetFolderPath = Paths.get("target/ignite/work/").toUri().getPath();
        ignite = Ignition.start(loadConfiguration("test-ignite.xml").get1().setWorkDirectory(targetFolderPath));
    }

    @AfterClass
    public static void tearDownClass() {
        ignite.close();
        Ignition.stop(true);
    }

    @Before
    public void setUp() {
        getIgniteCache = new GetIgniteCache() {
            @Override
            protected Ignite getIgnite() {
                return TestGetIgniteCache.ignite;
            }

        };
        properties1 = new HashMap<>();
        properties1.put("igniteKey", "key1");
        properties2 = new HashMap<>();
        properties2.put("igniteKey", "key2");
    }

    @After
    public void teardown() {
        getRunner = null;
        ignite.destroyCache(CACHE_NAME);
    }

    @Test
    public void testGetIgniteCacheDefaultConfOneFlowFileWithPlainKey() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "myKey");

        getRunner.assertValid();
        getRunner.enqueue(new byte[]{});

        getIgniteCache.initialize(getRunner.getProcessContext());

        getIgniteCache.getIgniteCache().put("myKey", "test".getBytes());

        getRunner.run(1, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> getSuccessfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, getSuccessfulFlowFiles.size());
        List<MockFlowFile> getFailureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, getFailureFlowFiles.size());

        final MockFlowFile getOut = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        getOut.assertContentEquals("test".getBytes());

        getRunner.shutdown();
    }

    @Test
    public void testGetIgniteCacheNullGetCacheThrowsException() {
        getIgniteCache = new GetIgniteCache() {
            @Override
            protected Ignite getIgnite() {
                return TestGetIgniteCache.ignite;
            }

            @Override
            protected IgniteCache<String, byte[]> getIgniteCache() {
                return null;
            }

        };
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "myKey");

        getRunner.assertValid();
        getRunner.enqueue(new byte[]{});

        getIgniteCache.initialize(getRunner.getProcessContext());

        getRunner.run(1, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 1);
        List<MockFlowFile> getSuccessfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, getSuccessfulFlowFiles.size());
        List<MockFlowFile> getFailureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, getFailureFlowFiles.size());

        final MockFlowFile getOut = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        getOut.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY,
                GetIgniteCache.IGNITE_GET_FAILED_MESSAGE_PREFIX + "java.lang.NullPointerException");

        getRunner.shutdown();
    }

    @Test
    public void testGetIgniteCacheDefaultConfOneFlowFileWithKeyExpression() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        getRunner.enqueue("".getBytes(), properties1);

        getIgniteCache.initialize(getRunner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key1", "test".getBytes());

        getRunner.run(1, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out.assertContentEquals("test".getBytes());
        getRunner.shutdown();
    }

    @Test
    public void testGetIgniteCacheDefaultConfTwoFlowFilesWithExpressionKeys() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        getRunner.enqueue("".getBytes(), properties1);
        getRunner.enqueue("".getBytes(), properties2);

        getIgniteCache.initialize(getRunner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key1", "test1".getBytes());
        getIgniteCache.getIgniteCache().put("key2", "test2".getBytes());

        getRunner.run(2, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);

        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out1.assertContentEquals("test1".getBytes());
        Assert.assertEquals("test1", new String(getIgniteCache.getIgniteCache().get("key1")));

        final MockFlowFile out2 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);

        out2.assertContentEquals("test2".getBytes());

        Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getIgniteCache().get("key2"));

        getRunner.shutdown();
    }

    @Test
    public void testGetIgniteCacheDefaultConfOneFlowFileNoKey() {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        properties1.clear();
        getRunner.enqueue("".getBytes(), properties1);
        getIgniteCache.initialize(getRunner.getProcessContext());

        getRunner.run(1, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 1);
        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);

        out.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        getRunner.shutdown();
    }


    @Test
    public void testGetIgniteCacheDefaultConfTwoFlowFilesNoKey() {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();

        properties1.clear();
        getRunner.enqueue("".getBytes(), properties1);
        getRunner.enqueue("".getBytes(), properties1);

        getIgniteCache.initialize(getRunner.getProcessContext());

        getRunner.run(2, false, true);

        getRunner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 2);
        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);
        final MockFlowFile out2 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(1);
        out2.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        getRunner.shutdown();

    }

    @Test
    public void testGetIgniteCacheDefaultConfTwoFlowFileFirstNoKey() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        getRunner.enqueue("".getBytes());
        getRunner.enqueue("".getBytes(), properties2);
        getIgniteCache.initialize(getRunner.getProcessContext());
        getIgniteCache.getIgniteCache().put("key2", "test2".getBytes());

        getRunner.run(2, false, true);

        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);

        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out2.assertContentEquals("test2".getBytes());
        Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getIgniteCache().get("key2"));

        getRunner.shutdown();
    }

    @Test
    public void testGetIgniteCacheDefaultConfTwoFlowFileSecondNoKey() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        getRunner.enqueue("".getBytes(), properties1);
        getRunner.enqueue("".getBytes());
        getIgniteCache.initialize(getRunner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key1", "test1".getBytes());
        getRunner.run(2, false, true);

        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);

        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out2.assertContentEquals("test1".getBytes());
        Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getIgniteCache().get("key1"));

        getRunner.shutdown();

    }


    @Test
    public void testGetIgniteCacheDefaultConfThreeFlowFilesOneOkSecondOkThirdNoExpressionKey() throws IOException {
        getRunner = TestRunners.newTestRunner(getIgniteCache);
        getRunner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        getRunner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        getRunner.assertValid();
        getRunner.enqueue("".getBytes(), properties1);
        getRunner.enqueue("".getBytes(), properties2);
        getRunner.enqueue("".getBytes());
        getIgniteCache.initialize(getRunner.getProcessContext());

        getIgniteCache.getIgniteCache().put("key1", "test1".getBytes());
        getIgniteCache.getIgniteCache().put("key2", "test2".getBytes());
        getRunner.run(3, false, true);

        List<MockFlowFile> successfulFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);

        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);

        out2.assertContentEquals("test1".getBytes());
        Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out3 = getRunner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);

        out3.assertContentEquals("test2".getBytes());
        Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getIgniteCache().get("key2"));

        getRunner.shutdown();

    }

}
