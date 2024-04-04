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
package org.apache.nifi.processors.asana;

import com.google.gson.Gson;
import org.apache.nifi.controller.asana.AsanaClientProviderService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.processors.asana.mocks.MockAsanaClientProviderService;
import org.apache.nifi.processors.asana.mocks.MockDistributedMapCacheClient;
import org.apache.nifi.processors.asana.mocks.MockGetAsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECTS;
import static org.apache.nifi.processors.asana.GetAsanaObject.ASANA_GID;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_CLIENT_SERVICE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_OBJECT_TYPE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_OUTPUT_BATCH_SIZE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_DISTRIBUTED_CACHE_SERVICE;
import static org.apache.nifi.processors.asana.GetAsanaObject.REL_NEW;
import static org.apache.nifi.processors.asana.GetAsanaObject.REL_REMOVED;
import static org.apache.nifi.processors.asana.GetAsanaObject.REL_UPDATED;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GetAsanaObjectLifecycleTest {

    private static final Gson GSON = new Gson();
    private TestRunner runner;
    private MockAsanaClientProviderService mockService;
    private MockDistributedMapCacheClient mockDistributedMapCacheClient;
    private AsanaObjectFetcher mockObjectFetcher;

    @BeforeEach
    public void init() {
        runner = newTestRunner(MockGetAsanaObject.class);
        mockService = new MockAsanaClientProviderService();
        mockDistributedMapCacheClient = new MockDistributedMapCacheClient();
        mockObjectFetcher = ((MockGetAsanaObject)runner.getProcessor()).objectFetcher;
    }

    @Test
    public void testYieldIsCalledWhenNoAsanaObjectsFetched() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        when(mockObjectFetcher.fetchNext()).thenReturn(null);

        runner.run(1);

        verify(mockObjectFetcher, times(1)).fetchNext();

        runner.assertTransferCount(REL_NEW, 0);
        runner.assertTransferCount(REL_REMOVED, 0);
        runner.assertTransferCount(REL_UPDATED, 0);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
    }

    @Test
    public void testCollectObjectsFromAsanaThenYield() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        when(mockObjectFetcher.fetchNext())
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "1", "Lorem ipsum"))
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "2", "dolor sit amet"))
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "3", "consectetur adipiscing elit"))
            .thenReturn(new AsanaObject(AsanaObjectState.UPDATED, "1", "Lorem Ipsum"))
            .thenReturn(new AsanaObject(AsanaObjectState.REMOVED, "3"))
            .thenReturn(null);

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 3);
        runner.assertTransferCount(REL_REMOVED, 1);
        runner.assertTransferCount(REL_UPDATED, 1);

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 3);
        runner.assertTransferCount(REL_REMOVED, 1);
        runner.assertTransferCount(REL_UPDATED, 1);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());

        verify(mockObjectFetcher, times(7)).fetchNext();

        final List<MockFlowFile> newFlowFiles = runner.getFlowFilesForRelationship(REL_NEW);

        newFlowFiles.get(0).assertAttributeEquals(ASANA_GID, "1");
        newFlowFiles.get(0).assertContentEquals("Lorem ipsum");

        newFlowFiles.get(1).assertAttributeEquals(ASANA_GID, "2");
        newFlowFiles.get(1).assertContentEquals("dolor sit amet");

        newFlowFiles.get(2).assertAttributeEquals(ASANA_GID, "3");
        newFlowFiles.get(2).assertContentEquals("consectetur adipiscing elit");

        final List<MockFlowFile> updatedFlowFiles = runner.getFlowFilesForRelationship(REL_UPDATED);

        updatedFlowFiles.getFirst().assertAttributeEquals(ASANA_GID, "1");
        updatedFlowFiles.getFirst().assertContentEquals("Lorem Ipsum");

        final List<MockFlowFile> removedFlowFiles = runner.getFlowFilesForRelationship(REL_REMOVED);

        removedFlowFiles.getFirst().assertAttributeEquals(ASANA_GID, "3");
    }

    @Test
    public void testCollectObjectsFromAsanaWithBatchSizeConfigured() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);
        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, "2");

        when(mockObjectFetcher.fetchNext())
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "1", GSON.toJson("Lorem ipsum")))
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "2", GSON.toJson("dolor sit amet")))
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "3", GSON.toJson("consectetur adipiscing elit")))
            .thenReturn(new AsanaObject(AsanaObjectState.UPDATED, "1", GSON.toJson("Lorem Ipsum")))
            .thenReturn(new AsanaObject(AsanaObjectState.REMOVED, "3", GSON.toJson("Some info about removal")))
            .thenReturn(null);

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 2);
        runner.assertTransferCount(REL_REMOVED, 1);
        runner.assertTransferCount(REL_UPDATED, 1);

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 2);
        runner.assertTransferCount(REL_REMOVED, 1);
        runner.assertTransferCount(REL_UPDATED, 1);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());

        verify(mockObjectFetcher, times(7)).fetchNext();

        final List<MockFlowFile> newFlowFiles = runner.getFlowFilesForRelationship(REL_NEW);

        newFlowFiles.get(0).assertContentEquals(GSON.toJson(asList("Lorem ipsum", "dolor sit amet")));
        newFlowFiles.get(1).assertContentEquals(GSON.toJson(singletonList("consectetur adipiscing elit")));

        final List<MockFlowFile> updatedFlowFiles = runner.getFlowFilesForRelationship(REL_UPDATED);

        updatedFlowFiles.getFirst().assertContentEquals(GSON.toJson(singletonList("Lorem Ipsum")));

        final List<MockFlowFile> removedFlowFiles = runner.getFlowFilesForRelationship(REL_REMOVED);

        removedFlowFiles.getFirst().assertContentEquals(GSON.toJson(singletonList("Some info about removal")));
    }

    @Test
    public void testAttemptLoadStateButNoStatePresent() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        when(mockObjectFetcher.fetchNext()).thenReturn(null);

        runner.run(1);

        verify(mockObjectFetcher, times(1)).loadState(emptyMap());
        verify(mockObjectFetcher, times(1)).clearState();
    }

    @Test
    public void testLoadValidState() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        final Map<String, String> validState = new LinkedHashMap<>();
        validState.put("Key1", "Value1");
        validState.put("Key2", "Value2");

        mockDistributedMapCacheClient.put(runner.getProcessor().getIdentifier(), validState);

        when(mockObjectFetcher.fetchNext()).thenReturn(null);

        runner.run(1);

        verify(mockObjectFetcher, times(1)).loadState(validState);
        verify(mockObjectFetcher, times(1)).clearState();
    }

    @Test
    public void testAttemptLoadInvalidStateThenClear() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        final Map<String, String> invalidState = singletonMap("Key", "Value");

        mockDistributedMapCacheClient.put(runner.getProcessor().getIdentifier(), invalidState);

        doThrow(new AsanaObjectFetcherException()).when(mockObjectFetcher).loadState(invalidState);
        when(mockObjectFetcher.fetchNext()).thenReturn(null);

        runner.run(1);

        verify(mockObjectFetcher, times(1)).loadState(invalidState);
        verify(mockObjectFetcher, times(2)).clearState();
    }

    @Test
    public void testStateIsSavedIfProcessorYields() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        final Map<String, String> state = singletonMap("Key", "Value");

        when(mockObjectFetcher.saveState()).thenReturn(state);
        when(mockObjectFetcher.fetchNext()).thenReturn(null);

        runner.run(1);

        assertEquals(state, mockDistributedMapCacheClient.get(runner.getProcessor().getIdentifier()));
    }

    @Test
    public void testStateIsSavedIfThereAreObjectsFetched() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        final Map<String, String> state = singletonMap("Key", "Value");

        when(mockObjectFetcher.saveState()).thenReturn(state);
        when(mockObjectFetcher.fetchNext())
            .thenReturn(new AsanaObject(AsanaObjectState.NEW, "1", "Lorem ipsum"))
            .thenReturn(null);

        runner.run(1);

        assertEquals(state, mockDistributedMapCacheClient.get(runner.getProcessor().getIdentifier()));
    }

    private void withMockAsanaClientService() throws InitializationException {
        final String serviceIdentifier = AsanaClientProviderService.class.getName();
        runner.addControllerService(serviceIdentifier, mockService);
        runner.enableControllerService(mockService);
        runner.setProperty(PROP_ASANA_CLIENT_SERVICE, serviceIdentifier);
    }

    private void withMockDistributedMapCacheClient() throws InitializationException {
        final String serviceIdentifier = DistributedMapCacheClient.class.getName();
        runner.addControllerService(serviceIdentifier, mockDistributedMapCacheClient);
        runner.enableControllerService(mockDistributedMapCacheClient);
        runner.setProperty(PROP_DISTRIBUTED_CACHE_SERVICE, serviceIdentifier);
    }
}
