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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutIgniteCache {

    private static final String CACHE_NAME = "testCache";
    private TestRunner runner;
    private PutIgniteCache putIgniteCache;
    private Map<String,String> properties1;
    private Map<String,String> properties2;
    private Ignite ignite;

    @BeforeEach
    public void setUp() {
        ignite = mock(Ignite.class);
        final IgniteCache igniteCache = TestGetIgniteCache.mockIgniteCache();
        when(igniteCache.getName()).thenReturn(CACHE_NAME);
        when(ignite.getOrCreateCache(or(ArgumentMatchers.eq(CACHE_NAME), isNull()))).thenReturn(igniteCache);
        final IgniteDataStreamer dataStreamer = mock(IgniteDataStreamer.class);
        final IgniteFuture igniteFuture = mock(IgniteFuture.class);
        when(igniteFuture.get()).thenReturn(new Object());

        final MutableBoolean allowOverride = new MutableBoolean(false);
        doAnswer(args -> {
            allowOverride.setValue((boolean) args.getArgument(0));
            return null;
        }).when(dataStreamer).allowOverwrite(anyBoolean());

        when(dataStreamer.addData(any(Collection.class))).thenAnswer(args -> {
            ((Collection)args.getArgument(0)).forEach(entry -> {
                final AbstractMap.SimpleEntry simpleEntry = (AbstractMap.SimpleEntry) entry;
                if (allowOverride.booleanValue() || !igniteCache.containsKey(simpleEntry.getKey())) {
                    igniteCache.put(simpleEntry.getKey(), simpleEntry.getValue());
                }
            });
            return igniteFuture;
        });
        when(ignite.dataStreamer(CACHE_NAME)).thenReturn(dataStreamer);
        putIgniteCache = new PutIgniteCache() {
            @Override
            protected Ignite getIgnite() {
                return ignite;
            }

        };
        properties1 = new HashMap<String,String>();
        properties1.put("igniteKey", "key1");
        properties2 = new HashMap<String,String>();
        properties2.put("igniteKey", "key2");
    }

    @AfterEach
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationOneFlowFileWithPlainKey() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "mykey");

        runner.assertValid();
        runner.enqueue("test".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out.assertContentEquals("test".getBytes());
        assertArrayEquals("test".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("mykey"));
        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationOneFlowFile() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 1);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out.assertContentEquals("test".getBytes());
        assertArrayEquals("test".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));
        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFilesAllowOverrideDefaultFalse() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test1".getBytes(),properties1);
        runner.enqueue("test2".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertEquals("test1",new String(putIgniteCache.getIgniteCache().get("key1")));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");

        out2.assertContentEquals("test2".getBytes());
        assertArrayEquals("test1".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));

        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFilesAllowOverrideTrue() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.DATA_STREAMER_ALLOW_OVERRIDE, "true");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test1".getBytes(),properties1);
        runner.enqueue("test2".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertEquals("test2",new String(putIgniteCache.getIgniteCache().get("key1")));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");

        out2.assertContentEquals("test2".getBytes());

        assertArrayEquals("test2".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));

        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationOneFlowFileNoKey() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        properties1.clear();
        runner.enqueue("test".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 1);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);

        out.assertContentEquals("test".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key1"));
        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationOneFlowFileNoBytes() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("".getBytes(),properties1);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 1);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");
        out.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);

        out.assertContentEquals("".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key1"));
        runner.shutdown();
    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFiles() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test1".getBytes(),properties1);
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_SUCCESS, 2);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(0, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertArrayEquals("test1".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "2");

        out2.assertContentEquals("test2".getBytes());
        assertArrayEquals("test2".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key2"));

        runner.shutdown();

    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFilesNoKey() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();

        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes());
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 2);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");

        out2.assertContentEquals("test2".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key2"));

        runner.shutdown();

    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFileFirstNoKey() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes(),properties2);
        runner.run(1, false, true);

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");

        out2.assertContentEquals("test2".getBytes());
        assertArrayEquals("test2".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key2"));

        runner.shutdown();

    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFileSecondNoKey() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();

        runner.enqueue("test1".getBytes(),properties1);
        runner.enqueue("test2".getBytes());
        runner.run(1, false, true);

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(1, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertArrayEquals("test1".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");

        out2.assertContentEquals("test2".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key2"));


        runner.shutdown();

    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFilesOneNoKeyOneNoBytes() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();

        runner.enqueue("test1".getBytes());
        runner.enqueue("".getBytes(),properties2);
        runner.run(1, false, true);

        runner.assertAllFlowFilesTransferred(PutIgniteCache.REL_FAILURE, 2);
        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");

        out2.assertContentEquals("".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key2"));

        runner.shutdown();

    }

    @Test
    public void testPutIgniteCacheOnTriggerDefaultConfigurationTwoFlowFilesOneNoKeySecondOkThirdNoBytes() throws IOException {
        runner = TestRunners.newTestRunner(putIgniteCache);
        runner.setProperty(PutIgniteCache.BATCH_SIZE, "5");
        runner.setProperty(PutIgniteCache.CACHE_NAME, CACHE_NAME);
        runner.setProperty(PutIgniteCache.DATA_STREAMER_PER_NODE_BUFFER_SIZE, "1");
        runner.setProperty(PutIgniteCache.IGNITE_CACHE_ENTRY_KEY, "${igniteKey}");

        runner.assertValid();
        runner.enqueue("test1".getBytes());
        runner.enqueue("test2".getBytes(),properties1);
        runner.enqueue("".getBytes(),properties2);
        runner.run(1, false, true);

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE);
        assertEquals(2, failureFlowFiles.size());

        final MockFlowFile out1 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(0);

        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "0");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
        out1.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "0");

        out1.assertContentEquals("test1".getBytes());
        assertEquals("test2", new String(putIgniteCache.getIgniteCache().get("key1")));

        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_SUCCESS).get(0);

        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, "0");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "1");
        out2.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, "1");

        out2.assertContentEquals("test2".getBytes());
        assertArrayEquals("test2".getBytes(),(byte[])putIgniteCache.getIgniteCache().get("key1"));

        final MockFlowFile out3 = runner.getFlowFilesForRelationship(PutIgniteCache.REL_FAILURE).get(1);

        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, "1");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, "3");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, "2");
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
        out3.assertAttributeEquals(PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, "2");

        out3.assertContentEquals("".getBytes());
        assertNull((byte[])putIgniteCache.getIgniteCache().get("key2"));

        runner.shutdown();

    }
}
