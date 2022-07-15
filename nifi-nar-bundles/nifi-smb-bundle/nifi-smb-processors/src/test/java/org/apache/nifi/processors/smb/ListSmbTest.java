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
package org.apache.nifi.processors.smb;

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.LISTING_STRATEGY;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_STATE_CACHE;
import static org.apache.nifi.processors.smb.ListSmb.DIRECTORY;
import static org.apache.nifi.processors.smb.ListSmb.MINIMUM_AGE;
import static org.apache.nifi.processors.smb.ListSmb.SHARE;
import static org.apache.nifi.processors.smb.ListSmb.SKIP_FILES_WITH_SUFFIX;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CONNECTION_POOL_SERVICE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.processor.util.list.ListedEntity;
import org.apache.nifi.services.smb.SmbSessionProviderService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ListSmbTest {

    private final static AtomicLong currentMillis = new AtomicLong();
    private final static AtomicLong currentNanos = new AtomicLong();

    private static long currentMillis() {
        return currentMillis.get();
    }

    private static long currentNanos() {
        return currentNanos.get();
    }

    private static void setTime(Long timeInMillis) {
        currentMillis.set(timeInMillis);
        currentNanos.set(NANOSECONDS.convert(timeInMillis, MILLISECONDS));
    }

    private static void timePassed(Long timeInMillis) {
        currentMillis.addAndGet(timeInMillis);
        currentNanos.addAndGet(NANOSECONDS.convert(timeInMillis, MILLISECONDS));
    }

    @Test
    public void shouldResetStateWhenPropertiesChanged() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        final NiFiSmbClient mockNifiSmbjClient = configureTestRunnerWithMockedSambaClient(testRunner);
        long now = System.currentTimeMillis();
        mockSmbFolders(mockNifiSmbjClient, listableEntity("should_list_it_after_each_reset", now - 100));
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.setProperty(DIRECTORY, "testDirectoryChanged");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 2);

        testRunner.setProperty(SKIP_FILES_WITH_SUFFIX, "suffix_changed");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 3);

        final SmbSessionProviderService connectionPoolService = mock(SmbSessionProviderService.class);
        when(connectionPoolService.getIdentifier()).thenReturn("connection-pool-2");
        when(connectionPoolService.getServiceLocation()).thenReturn(URI.create("smb://localhost:443/share"));
        final NiFiSmbClientFactory mockSmbClientFactory = mock(NiFiSmbClientFactory.class);
        ((ListSmb) testRunner.getProcessor()).smbClientFactory = mockSmbClientFactory;
        when(mockSmbClientFactory.create(any(), any())).thenReturn(mockNifiSmbjClient);
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, "connection-pool-2");
        testRunner.addControllerService("connection-pool-2", connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);

        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 4);

        testRunner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 50L, 150L, 3000L})
    public void testShouldUseTimestampBasedStrategyProperly(Long minimumAge) throws Exception {
        final TestRunner testRunner = newTestRunner(TimeMockingListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, minimumAge + " ms");
        final NiFiSmbClient mockNifiSmbjClient = configureTestRunnerWithMockedSambaClient(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbjClient);
        testRunner.run();
        verify(mockNifiSmbjClient).close();
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 900),
                listableEntity("second", 1000)
        );
        testRunner.run();
        timePassed(100L);
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 900),
                listableEntity("second", 1000),
                listableEntity("third", 1100)
        );
        testRunner.run();
        timePassed(1L);
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 900),
                listableEntity("second", 1000),
                listableEntity("third", 1100),
                listableEntity("appeared_during_the_previous_iteration_and_it_was_missed", 1099)
        );
        timePassed(100L);
        testRunner.run();
        timePassed(minimumAge);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 4);
        testRunner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 50L, 150L, 3000L})
    public void testShouldUseEntityTrackingBasedStrategyProperly(Long minimumAge) throws Exception {
        final TestRunner testRunner = newTestRunner(TimeMockingListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "entities");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, minimumAge + " ms");
        final DistributedMapCacheClient cacheService = mockDistributedMap();
        testRunner.addControllerService("cacheService", cacheService);
        testRunner.setProperty(TRACKING_STATE_CACHE, "cacheService");
        testRunner.enableControllerService(cacheService);
        final NiFiSmbClient mockNifiSmbjClient = configureTestRunnerWithMockedSambaClient(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 1000)
        );
        testRunner.run();
        verify(mockNifiSmbjClient).close();
        timePassed(100L);
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 1000),
                listableEntity("second", 1100)
        );
        testRunner.run();
        timePassed(minimumAge);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 2);
        testRunner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 50L, 150L, 3000L})
    public void testShouldUseNoTrackingBasedStrategyProperly(Long minimumAge) throws Exception {
        final TestRunner testRunner = newTestRunner(TimeMockingListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, minimumAge + " ms");
        final NiFiSmbClient mockNifiSmbjClient = configureTestRunnerWithMockedSambaClient(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbjClient,
                listableEntity("first", 1000)
        );
        timePassed(minimumAge);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 2);
        testRunner.assertValid();
    }

    @Test
    public void shouldTurnSmbClientExceptionsToBulletins() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        final NiFiSmbClient mockNifiSmbjClient = configureTestRunnerWithMockedSambaClient(testRunner);
        when(mockNifiSmbjClient.listRemoteFiles(anyString())).thenThrow(new RuntimeException("test exception"));
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
    }

    @Test
    public void shouldFormatRemotePathProperly() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbSessionProviderService connectionPoolService = mockSmbConnectionPoolService();
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, "connection-pool");
        testRunner.setProperty(SHARE, "share");
        testRunner.addControllerService("connection-pool", connectionPoolService);
        when(connectionPoolService.getServiceLocation()).thenReturn(URI.create("smb://hostname:443"));
        final ListSmb underTest = (ListSmb) testRunner.getProcessor();

        assertEquals("smb://hostname:443/share:\\", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "\\");
        assertEquals("smb://hostname:443/share:\\", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "root");
        assertEquals("smb://hostname:443/share:\\root\\", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "root\\subdirectory");
        assertEquals("smb://hostname:443/share:\\root\\subdirectory\\", underTest.getPath(testRunner.getProcessContext()));

        testRunner.removeProperty(SHARE);

        testRunner.setProperty(DIRECTORY, "");
        assertEquals("smb://hostname:443:\\", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "root");
        assertEquals("smb://hostname:443:\\root\\", underTest.getPath(testRunner.getProcessContext()));
    }

    private SmbSessionProviderService mockSmbConnectionPoolService() {
        final SmbSessionProviderService connectionPoolService = mock(SmbSessionProviderService.class);
        when(connectionPoolService.getIdentifier()).thenReturn("connection-pool");
        when(connectionPoolService.getServiceLocation()).thenReturn(URI.create("smb://localhost:443/share"));
        return connectionPoolService;
    }

    private NiFiSmbClient configureTestRunnerWithMockedSambaClient(TestRunner testRunner)
            throws Exception {
        final NiFiSmbClient mockNifiSmbjClient = mock(NiFiSmbClient.class);
        testRunner.setProperty(DIRECTORY, "testDirectory");

        final SmbSessionProviderService connectionPoolService = mockSmbConnectionPoolService();
        final NiFiSmbClientFactory mockSmbClientFactory = mock(NiFiSmbClientFactory.class);
        ((ListSmb) testRunner.getProcessor()).smbClientFactory = mockSmbClientFactory;
        when(mockSmbClientFactory.create(any(), any())).thenReturn(mockNifiSmbjClient);
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, "connection-pool");
        testRunner.addControllerService("connection-pool", connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);

        return mockNifiSmbjClient;
    }

    private void mockSmbFolders(NiFiSmbClient mockNifiSmbjClient, SmbListableEntity... entities) {
        doAnswer(ignore -> stream(entities)).when(mockNifiSmbjClient).listRemoteFiles(anyString());
    }

    private SmbListableEntity listableEntity(String name, long timeStamp) {
        return SmbListableEntity.builder()
                .setName(name)
                .setTimestamp(timeStamp)
                .build();
    }

    private DistributedMapCacheClient mockDistributedMap() throws IOException {
        final Map<String, ConcurrentHashMap<String, ListedEntity>> store = new ConcurrentHashMap<>();
        final DistributedMapCacheClient cacheService = mock(DistributedMapCacheClient.class);
        when(cacheService.putIfAbsent(any(), any(), any(), any())).thenReturn(false);
        when(cacheService.containsKey(any(), any())).thenReturn(false);
        when(cacheService.getIdentifier()).thenReturn("cacheService");
        doAnswer(invocation -> store.get(invocation.getArgument(0)))
                .when(cacheService).get(any(), any(), any());
        doAnswer(invocation -> store.put(invocation.getArgument(0), invocation.getArgument(1)))
                .when(cacheService).put(any(), any(), any(), any());
        doAnswer(invocation -> Optional.ofNullable(invocation.getArgument(0)).map(store::remove).isPresent())
                .when(cacheService).remove(any(), any());
        return cacheService;
    }

    public static class TimeMockingListSmb extends ListSmb {

        @Override
        protected long getCurrentTime() {
            return currentMillis();
        }

        @Override
        protected long getCurrentNanoTime() {
            return currentNanos();
        }
    }
}