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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
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
import static org.junit.Assert.assertNull;

public class TestPutIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private static Ignite thickIgniteClient;
    private TestRunner runner;
    private PutIgniteCache putIgniteCache;
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
        putIgniteCache = new PutIgniteCache() {
            @Override
            protected Ignite getThickIgniteClient() {
                return TestPutIgniteCache.thickIgniteClient;
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
    public void putIgniteCache_oneFlowFilePlainKey_putUsingThickIgniteClient() throws IOException {
        assertOneFlowFilePlainKeyPut(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_oneFlowFilePlainKey_putUsingThinIgniteClient() throws IOException {
        assertOneFlowFilePlainKeyPut(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_oneFlowFileExpressionKey_putUsingThickIgniteClient() throws IOException {
        assertOneFlowFileExpressionKeyPut(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_oneFlowFileExpressionKey_putUsingThinIgniteClient() throws IOException {
        assertOneFlowFileExpressionKeyPut(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeysFalseAllowOverwrite_putNotUpdatedUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysFalseAllowOverwritePutNotUpdated(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeysFalseAllowOverwrite_putNotUpdatedUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysFalseAllowOverwritePutNotUpdated(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeysTrueAllowOverwrite_putUpdatedUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysTrueAllowOverwritePutUpdated(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeysTrueAllowOverwrite_putUpdatedUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysTrueAllowOverwritePutUpdated(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_oneFlowFileNoKey_routedToFailureUsingThickIgniteClient() throws IOException {
        assertOneFlowFileNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_oneFlowFileNoKey_routedToFailureUsingThinIgniteClient() throws IOException {
        assertOneFlowFileNoKeyRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_oneFlowFileZeroBytes_routedToFailureUsingThickIgniteClient() throws IOException {
        assertOneFlowFileZeroBytesRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_oneFlowFileZeroBytes_routedToFailureUsingThinIgniteClient() throws IOException {
        assertOneFlowFileZeroBytesRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeys_putUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysPut(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_twoFlowFilesExpressionKeys_putUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesExpressionKeysPut(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_twoFlowFilesNoKeys_routedToFailureUsingThickIgniteClient() throws IOException {
        assertTwoFlowFilesNoKeysRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_twoFlowFilesNoKeys_routedToFailureUsingThinIgniteClient() throws IOException {
        assertTwoFlowFilesNoKeysRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKey_firstFlowFileRoutedToFailureSecondFlowFilePutUsingThickIgniteClient() throws IOException {
        assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyPut(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKey_firstFlowFileRoutedToFailureSecondFlowFilePutUsingThinIgniteClient() throws IOException {
        assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyPut(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_firstFlowFileExpressionKeySecondFlowFileNoKey_firstFlowFilePutSecondFlowFileRoutedToFailureUsingThickIgniteClient() throws IOException {
        assertFlowFileExpressionKeyPutFlowFileNoKeyRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_firstFlowFileExpressionKeySecondFlowFileNoKey_firstFlowFilePutSecondFlowFileRoutedToFailureUsingThinIgniteClient() throws IOException {
        assertFlowFileExpressionKeyPutFlowFileNoKeyRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileZeroBytes_routedToFailureUsingThickIgniteClient() throws IOException {
        assertFlowFileNoKeyFlowFileZeroBytesRoutedToFailure(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileZeroBytes_routedToFailureUsingThinIgniteClient() throws IOException {
        assertFlowFileNoKeyFlowFileZeroBytesRoutedToFailure(ClientType.THIN);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKeyThirdFlowFileZeroBytes_firstThirdFlowFilesRoutedToFailureSecondFlowFilePutUsingThickIgniteClient() throws IOException {
        assertFirstFlowFileNoKeyThirdFlowFileZeroBytesRoutedToFailureSecondFlowFileExpressionKeyPut(ClientType.THICK);
    }

    @Test
    public void putIgniteCache_firstFlowFileNoKeySecondFlowFileExpressionKeyThirdFlowFileZeroBytes_firstThirdFlowFilesRoutedToFailureSecondFlowFilePutUsingThinIgniteClient() throws IOException {
        assertFirstFlowFileNoKeyThirdFlowFileZeroBytesRoutedToFailureSecondFlowFileExpressionKeyPut(ClientType.THIN);
    }

    private void assertOneFlowFilePlainKeyPut(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "myKey");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
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
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThickIgniteClientCache().get("myKey"));
        } else {
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThinIgniteClientCache().get("myKey"));
        }

        runner.shutdown();
    }

    private void assertOneFlowFileExpressionKeyPut(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
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
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        runner.shutdown();
    }

    private void assertTwoFlowFilesExpressionKeysFalseAllowOverwritePutNotUpdated(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes(), properties1);
        runner.enqueue("test2".getBytes(), properties1);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertEquals("test1", new String(putIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            assertEquals("test1", new String(putIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertEquals("test1", new String(putIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            assertEquals("test1", new String(putIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        runner.shutdown();
    }

    private void assertTwoFlowFilesExpressionKeysTrueAllowOverwritePutUpdated(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.ALLOW_OVERWRITE, "true");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes(), properties1);
        runner.enqueue("test2".getBytes(), properties1);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertEquals("test2", new String(putIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            assertEquals("test2", new String(putIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        runner.shutdown();
    }

    private void assertOneFlowFileNoKeyRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        properties1.clear();
        runner.enqueue("test".getBytes(), properties1);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out.assertContentEquals("test".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        runner.shutdown();
    }

    private void assertOneFlowFileZeroBytesRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("".getBytes(), properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 1);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
        out.assertContentEquals("".getBytes());

        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        runner.shutdown();
    }

    private void assertTwoFlowFilesExpressionKeysPut(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes(), properties1);
        runner.enqueue("test2".getBytes(), properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertTwoFlowFilesNoKeysRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes());

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertFlowFileNoKeyRoutedToFailureFlowFileExpressionKeyPut(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes(), properties2);

        runner.run(1, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test2".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertFlowFileExpressionKeyPutFlowFileNoKeyRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes(), properties1);
        runner.enqueue("test2".getBytes());

        runner.run(1, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            Assert.assertArrayEquals("test1".getBytes(), putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertFlowFileNoKeyFlowFileZeroBytesRoutedToFailure(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("".getBytes(), properties2);

        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 2);
        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key1"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key1"));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertContentEquals("".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }

    private void assertFirstFlowFileNoKeyThirdFlowFileZeroBytesRoutedToFailureSecondFlowFileExpressionKeyPut(final ClientType clientType) throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");
        runner.setProperty(PutIgniteCache.IGNITE_CLIENT_TYPE, clientType.name());

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes(), properties1);
        runner.enqueue("".getBytes(), properties2);

        runner.run(1, false, true);

        final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out1.assertContentEquals("test1".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertEquals("test2", new String(putIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            assertEquals("test2", new String(putIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out2.assertContentEquals("test2".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertEquals("test2", new String(putIgniteCache.getThickIgniteClientCache().get("key1")));
        } else if (clientType.equals(ClientType.THIN)) {
            assertEquals("test2", new String(putIgniteCache.getThinIgniteClientCache().get("key1")));
        }

        final MockFlowFile out3 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "2");
        out3.assertContentEquals("".getBytes());
        if (clientType.equals(ClientType.THICK)) {
            assertNull(putIgniteCache.getThickIgniteClientCache().get("key2"));
        } else if (clientType.equals(ClientType.THIN)) {
            assertNull(putIgniteCache.getThinIgniteClientCache().get("key2"));
        }

        runner.shutdown();
    }
}
