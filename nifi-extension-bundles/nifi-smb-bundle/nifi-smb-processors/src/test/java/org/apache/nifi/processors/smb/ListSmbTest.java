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
import static org.apache.nifi.processor.util.list.AbstractListProcessor.BY_TIMESTAMPS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.LISTING_STRATEGY;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_STATE_CACHE;
import static org.apache.nifi.processors.smb.ListSmb.DIRECTORY;
import static org.apache.nifi.processors.smb.ListSmb.IGNORE_FILES_WITH_SUFFIX;
import static org.apache.nifi.processors.smb.ListSmb.INITIAL_LISTING_STRATEGY;
import static org.apache.nifi.processors.smb.ListSmb.INITIAL_LISTING_TIMESTAMP;
import static org.apache.nifi.processors.smb.ListSmb.MAXIMUM_AGE;
import static org.apache.nifi.processors.smb.ListSmb.MINIMUM_AGE;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.processors.smb.ListSmb.SMB_LISTING_STRATEGY;
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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.list.ListedEntity;
import org.apache.nifi.processors.smb.util.InitialListingStrategy;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ListSmbTest {

    private final static AtomicLong currentMillis = new AtomicLong();
    private final static AtomicLong currentNanos = new AtomicLong();
    public static final String CLIENT_SERVICE_PROVIDER_ID = "client-provider-service-id";

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

    @ParameterizedTest
    @ValueSource(strings = {"timestamps", "entities"})
    public void shouldResetStateWhenPropertiesChanged(String listingStrategy) throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, listingStrategy);
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        final DistributedMapCacheClient cacheService = mockDistributedMap();
        testRunner.addControllerService("cacheService", cacheService);
        testRunner.setProperty(TRACKING_STATE_CACHE, "cacheService");
        testRunner.enableControllerService(cacheService);
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        long now = System.currentTimeMillis();
        mockSmbFolders(mockNifiSmbClientService,
                listableEntity("should_list_this_again_after_property_change", now - 100));
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.clearTransferState();
        testRunner.setProperty(DIRECTORY, "testDirectoryChanged");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.clearTransferState();

        testRunner.setProperty(IGNORE_FILES_WITH_SUFFIX, "suffix_changed");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.clearTransferState();

        final SmbClientProviderService clientProviderService = mock(SmbClientProviderService.class);
        when(clientProviderService.getIdentifier()).thenReturn("different-client-provider");
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://localhost:445/share"));
        when(clientProviderService.getClient(any(ComponentLog.class))).thenReturn(mockNifiSmbClientService);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, "different-client-provider");
        testRunner.addControllerService("different-client-provider", clientProviderService);
        testRunner.enableControllerService(clientProviderService);

        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 50L, 150L, 3000L})
    public void testShouldUseTimestampBasedStrategyProperly(Long minimumAge) throws Exception {
        final TestRunner testRunner = newTestRunner(TimeMockingListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, minimumAge + " ms");
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbClientService);
        testRunner.run();
        verify(mockNifiSmbClientService).close();
        mockSmbFolders(mockNifiSmbClientService,
                listableEntity("first", 900),
                listableEntity("second", 1000)
        );
        testRunner.run();
        timePassed(100L);
        mockSmbFolders(mockNifiSmbClientService,
                listableEntity("first", 900),
                listableEntity("second", 1000),
                listableEntity("third", 1100)
        );
        testRunner.run();
        timePassed(1L);
        mockSmbFolders(mockNifiSmbClientService,
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
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbClientService,
                listableEntity("first", 1000)
        );
        testRunner.run();
        verify(mockNifiSmbClientService).close();
        timePassed(100L);
        mockSmbFolders(mockNifiSmbClientService,
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
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbClientService,
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
    public void testShouldFilterByFileAgeCriteria() throws Exception {
        final TestRunner testRunner = newTestRunner(TimeMockingListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        testRunner.setProperty(MINIMUM_AGE, "10 ms");
        testRunner.setProperty(MAXIMUM_AGE, "30 ms");
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        setTime(1000L);
        mockSmbFolders(mockNifiSmbClientService,
                listableEntity("too_young", 1000),
                listableEntity("too_old", 1000 - 31),
                listableEntity("should_list_this", 1000 - 11)
        );
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertValid();
    }

    @Test
    public void shouldTurnSmbClientExceptionsToBulletins() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, "millis");
        final SmbClientService mockNifiSmbClientService = configureTestRunnerWithMockedSmbClientService(testRunner);
        when(mockNifiSmbClientService.listFiles(anyString())).thenThrow(new RuntimeException("test exception"));
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
    }

    @Test
    public void shouldFormatRemotePathProperly() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbClientProviderService clientProviderService = mockSmbClientProviderService();
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_PROVIDER_ID);
        testRunner.addControllerService(CLIENT_SERVICE_PROVIDER_ID, clientProviderService);
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://hostname:445/share"));
        final ListSmb underTest = (ListSmb) testRunner.getProcessor();

        assertEquals("smb://hostname:445/share/", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "/");
        assertEquals("smb://hostname:445/share/", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "root");
        assertEquals("smb://hostname:445/share/root/", underTest.getPath(testRunner.getProcessContext()));

        testRunner.setProperty(DIRECTORY, "root/subdirectory");
        assertEquals("smb://hostname:445/share/root/subdirectory/", underTest.getPath(testRunner.getProcessContext()));

    }

    @Test
    void testInitialListingTimestampValidEpoch() throws Exception {
        testInitialListingTimestamp("123456789", true);
    }

    @Test
    void testInitialListingTimestampValidUTCTime() throws Exception {
        testInitialListingTimestamp("2025-04-03T16:16:54Z", true);
    }

    @Test
    void testInitialListingTimestampInvalidTime() throws Exception {
        testInitialListingTimestamp("2025-04-03 16:16:54", false);
    }

    private void testInitialListingTimestamp(String propertyValue, boolean shouldBeValid) throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);

        configureTestRunnerWithMockedSmbClientService(testRunner);

        testRunner.setProperty(SMB_LISTING_STRATEGY, BY_TIMESTAMPS);
        testRunner.setProperty(INITIAL_LISTING_STRATEGY, InitialListingStrategy.FROM_TIMESTAMP);
        testRunner.setProperty(INITIAL_LISTING_TIMESTAMP, propertyValue);

        if (shouldBeValid) {
            testRunner.assertValid();
        } else {
            testRunner.assertNotValid();
        }
    }

    private SmbClientProviderService mockSmbClientProviderService() {
        final SmbClientProviderService clientProviderService = mock(SmbClientProviderService.class);
        when(clientProviderService.getIdentifier()).thenReturn(CLIENT_SERVICE_PROVIDER_ID);
        when(clientProviderService.getServiceLocation()).thenReturn(URI.create("smb://localhost:445/share"));
        return clientProviderService;
    }

    private SmbClientService configureTestRunnerWithMockedSmbClientService(TestRunner testRunner)
            throws Exception {
        final SmbClientService mockNifiSmbClientService = mock(SmbClientService.class);
        testRunner.setProperty(DIRECTORY, "testDirectory");

        final SmbClientProviderService clientProviderService = mockSmbClientProviderService();
        when(clientProviderService.getClient(any(ComponentLog.class))).thenReturn(mockNifiSmbClientService);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, CLIENT_SERVICE_PROVIDER_ID);
        testRunner.addControllerService(CLIENT_SERVICE_PROVIDER_ID, clientProviderService);
        testRunner.enableControllerService(clientProviderService);

        return mockNifiSmbClientService;
    }

    private void mockSmbFolders(SmbClientService mockNifiSmbClientService, SmbListableEntity... entities) {
        doAnswer(ignore -> stream(entities)).when(mockNifiSmbClientService).listFiles(anyString());
    }

    private SmbListableEntity listableEntity(String name, long timeStamp) {
        return SmbListableEntity.builder()
                .setName(name)
                .setLastModifiedTime(timeStamp)
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