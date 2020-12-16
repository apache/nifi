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
package org.apache.nifi.hazelcast.services.cachemanager;

import org.apache.nifi.hazelcast.services.cacheclient.HazelcastMapCacheClient;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public abstract class AbstractHazelcastCacheManagerTest {

    protected TestHazelcastProcessor processor;
    protected TestRunner testRunner;
    protected  HazelcastMapCacheClient hazelcastMapCacheClient;
    protected IMapBasedHazelcastCacheManager testSubject;

    @Before
    public void setUp() {
        processor = new TestHazelcastProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @After
    public void tearDown() {
        testRunner.disableControllerService(hazelcastMapCacheClient);
        testRunner.disableControllerService(testSubject);
        testRunner.shutdown();
    }

    protected void givenHazelcastMapCacheClient() throws Exception {
        hazelcastMapCacheClient = new HazelcastMapCacheClient();
        testRunner.addControllerService("hazelcast-map-cache-client", hazelcastMapCacheClient);

        testRunner.setProperty(hazelcastMapCacheClient, HazelcastMapCacheClient.HAZELCAST_ENTRY_TTL, "20 sec");
        testRunner.setProperty(hazelcastMapCacheClient, HazelcastMapCacheClient.HAZELCAST_CACHE_NAME, "cache");
        testRunner.setProperty(hazelcastMapCacheClient, HazelcastMapCacheClient.HAZELCAST_CACHE_MANAGER, "hazelcast-connection-service");

        testRunner.setProperty(TestHazelcastProcessor.TEST_HAZELCAST_MAP_CACHE_CLIENT, "hazelcast-map-cache-client");
    }

    protected void givenServicesAreEnabled() {
        testRunner.enableControllerService(testSubject);
        Assert.assertTrue(testSubject.isEnabled());

        testRunner.enableControllerService(hazelcastMapCacheClient);
        Assert.assertTrue(hazelcastMapCacheClient.isEnabled());
    }

    protected void whenExecuting() {
        testRunner.enqueue("trigger");
        testRunner.run();
    }

    protected void thenProcessingIsSuccessful() {
        testRunner.assertAllFlowFilesTransferred(TestHazelcastProcessor.REL_SUCCESS, 1);
    }
}
