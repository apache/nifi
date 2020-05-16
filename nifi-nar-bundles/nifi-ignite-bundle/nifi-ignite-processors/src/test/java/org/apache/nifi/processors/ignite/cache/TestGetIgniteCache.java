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
import org.apache.ignite.client.ClientCache;
import org.apache.nifi.processors.ignite.ClientType;
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
    private static Ignite thickIgniteClient;
    private TestRunner runner;
    private GetIgniteCache getIgniteCache;
    private Map<String, String> properties1;
    private Map<String, String> properties2;

    @BeforeClass
    public static void setUpClass() throws IgniteCheckedException {
        final List<Ignite> grids = Ignition.allGrids();
        if (grids.size() == 1) {
            thickIgniteClient = grids.get(0);
        } else {
            final String targetFolderPath = Paths.get("target/ignite/work/").toUri().getPath();
            thickIgniteClient = Ignition.start(loadConfiguration("test-ignite-client.xml").get1().setWorkDirectory(targetFolderPath));
        }
    }

    @AfterClass
    public static void tearDownClass() {
        thickIgniteClient.close();
        Ignition.stop(true);
    }

    @Before
    public void setUp() {
        getIgniteCache = new GetIgniteCache() {
            @Override
            protected Ignite getThickIgniteClient() {
                return TestGetIgniteCache.thickIgniteClient;
            }
        };
        properties1 = new HashMap<>();
        properties1.put("igniteKey", "key1");
        properties2 = new HashMap<>();
        properties2.put("igniteKey", "key2");
    }

    @After
    public void teardown() {
        runner = null;
        thickIgniteClient.destroyCache(CACHE_NAME);
    }

    @Test
    public void getIgniteCache_oneFlowFilePlainKey_retrievedUsingThickIgniteClient() throws IOException {
        assertOneFlowFilePlainKeyRetrieved(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_oneFlowFilePlainKey_retrievedUsingThinIgniteClient() throws IOException {
        assertOneFlowFilePlainKeyRetrieved(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_nullThickIgniteClientCache_throwsNullPointerException() {
        getIgniteCache = new GetIgniteCache() {
            @Override
            protected Ignite getThickIgniteClient() {
                return TestGetIgniteCache.thickIgniteClient;
            }

            @Override
            protected IgniteCache<String, byte[]> getThickIgniteClientCache() {
                return null;
            }
        };
        assertNullPointerExceptionThrownWhenIgniteClientCacheIsNull(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_nullThinIgniteClientCache_throwsNullPointerException() {
        getIgniteCache = new GetIgniteCache() {
            @Override
            protected ClientCache<String, byte[]> getThinIgniteClientCache() {
                return null;
            }
        };
        assertNullPointerExceptionThrownWhenIgniteClientCacheIsNull(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_oneFlowFileExpressionKey_retrievedUsingThickIgniteClient() throws IOException {
        assertOneFlowFileExpressionKeyRetrieved(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_oneFlowFileExpressionKey_retrievedUsingThinIgniteClient() throws IOException {
        assertOneFlowFileExpressionKeyRetrieved(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_twoFlowFilesExpressionKeys_retrievedUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysRetrieved(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_twoFlowFilesExpressionKeys_retrievedUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysRetrieved(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_oneFlowFileNoKey_routedToFailureUsingThickIgniteClient() {
        assertOneFlowFileNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_oneFlowFileNoKey_routedToFailureUsingThinIgniteClient() {
        assertOneFlowFileNoKeyRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_twoFlowFilesNoKey_routedToFailureUsingThickIgniteClient() {
        assertTwoFlowFilesNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_twoFlowFilesNoKey_routedToFailureUsingThinIgniteClient() {
        assertTwoFlowFilesNoKeyRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKey_firstFlowFileRoutedToFailureSecondFlowFileRetrievedUsingThickIgniteClient() throws IOException {
        assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyRetrieved(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKey_firstFlowFileRoutedToFailureSecondFlowFileRetrievedUsingThinIgniteClient() throws IOException {
        assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyRetrieved(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_firstFlowFileExpressionKeySecondFlowFileNoKey_firstFlowFileRetrievedSecondFlowFileRoutedToFailureUsingThickIgniteClient() throws IOException {
        assertFlowFileExpressionKeyRetrievedFlowFileNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_firstFlowFileExpressionKeySecondFlowFileNoKey_firstFlowFileRetrievedSecondFlowFileRoutedToFailureUsingThinIgniteClient() throws IOException {
        assertFlowFileExpressionKeyRetrievedFlowFileNoKeyRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void getIgniteCache_firstSecondFlowFilesExpressionKeysThirdFlowFileNoKey_firstSecondFlowFilesRetrievedThirdFlowFileRoutedToFailureUsingThickIgniteClient() throws IOException {
        assertFlowFilesExpressionKeysRetrievedFlowFileNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void getIgniteCache_firstSecondFlowFilesExpressionKeysThirdFlowFileNoKey_firstSecondFlowFilesRetrievedThirdFlowFileRoutedToFailureUsingThinIgniteClient() throws IOException {
        assertFlowFilesExpressionKeysRetrievedFlowFileNoKeyRoutedToFailure(ClientType.THIN);
    }

    private void assertOneFlowFilePlainKeyRetrieved(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "myKey");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue(new byte[]{});

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("myKey", "test".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("myKey", "test".getBytes());
        }

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        final List<MockFlowFile> getSuccessfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, getSuccessfulFlowFiles.size());
        final List<MockFlowFile> getFailureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, getFailureFlowFiles.size());

        final MockFlowFile getOut = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        getOut.assertContentEquals("test".getBytes());

        runner.shutdown();
    }

    private void assertNullPointerExceptionThrownWhenIgniteClientCacheIsNull(final ClientType clientType) {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "myKey");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue(new byte[]{});

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 1);
        final List<MockFlowFile> getSuccessfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, getSuccessfulFlowFiles.size());
        final List<MockFlowFile> getFailureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, getFailureFlowFiles.size());

        final MockFlowFile getOut = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        getOut.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MESSAGE_PREFIX + "java.lang.NullPointerException");

        runner.shutdown();
    }

    private void assertOneFlowFileExpressionKeyRetrieved(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes(), properties1);

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key1", "test".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key1", "test".getBytes());
        }

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out.assertContentEquals("test".getBytes());

        runner.shutdown();
    }

    private void assertTwoFlowFilesExpressionKeysRetrieved(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes(), properties1);
        runner.enqueue("".getBytes(), properties2);

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key1", "test1".getBytes());
            getIgniteCache.getThickIgniteClientCache().put("key2", "test2".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key1", "test1".getBytes());
            getIgniteCache.getThinIgniteClientCache().put("key2", "test2".getBytes());
        }

        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertEquals("test1", new String(getIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertEquals("test1", new String(getIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertOneFlowFileNoKeyRoutedToFailure(final ClientType clientType) {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.clear();
        runner.enqueue("".getBytes(), properties1);
        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        runner.shutdown();
    }

    private void assertTwoFlowFilesNoKeyRoutedToFailure(final ClientType clientType) {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();

        properties1.clear();
        runner.enqueue("".getBytes(), properties1);
        runner.enqueue("".getBytes(), properties1);

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        runner.run(2, false, true);

        runner.assertAllFlowFilesTransferred(GetIgniteCache.REL_FAILURE, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(1);
        out2.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        runner.shutdown();
    }

    private void assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyRetrieved(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes());
        runner.enqueue("".getBytes(), properties2);

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key2", "test2".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key2", "test2".getBytes());
        }

        runner.run(2, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertFlowFileExpressionKeyRetrievedFlowFileNoKeyRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes(), properties1);
        runner.enqueue("".getBytes());

        getIgniteCache.initializeGetIgniteCacheProcessor(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key1", "test1".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key1", "test1".getBytes());
        }

        runner.run(2, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out2.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        runner.shutdown();
    }

    private void assertFlowFilesExpressionKeysRetrievedFlowFileNoKeyRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(getIgniteCache);
        runner.setProperty(GetIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(GetIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(GetIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes(), properties1);
        runner.enqueue("".getBytes(), properties2);
        runner.enqueue("".getBytes());

        getIgniteCache.initializeIgniteCache(runner.getProcessContext());

        if (clientType.equals(ClientType.THICK)) {
            getIgniteCache.getThickIgniteClientCache().put("key1", "test1".getBytes());
            getIgniteCache.getThickIgniteClientCache().put("key2", "test2".getBytes());
        } else if (clientType.equals(ClientType.THIN)) {
            getIgniteCache.getThinIgniteClientCache().put("key1", "test1".getBytes());
            getIgniteCache.getThinIgniteClientCache().put("key2", "test2".getBytes());
        }

        runner.run(3, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_FAILURE).get(0);
        out1.assertContentEquals("".getBytes());
        out1.assertAttributeEquals(GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, GetIgniteCache.IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(0);
        out2.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test1".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out3 = runner.getFlowFilesForRelationship(GetIgniteCache.REL_SUCCESS).get(1);
        out3.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), getIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }
}
