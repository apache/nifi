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
package org.apache.nifi.web.configuration;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.BulkReplayJobStore;
import org.apache.nifi.web.api.VolatileBulkReplayJobStore;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBulkReplayJobStoreConfiguration {

    @Test
    void testBulkReplayJobStore_returnsVolatileImplementation() {
        final BulkReplayJobStoreConfiguration configuration = createConfiguration();
        final BulkReplayJobStore store = configuration.bulkReplayJobStore();

        assertInstanceOf(VolatileBulkReplayJobStore.class, store);
    }

    @Test
    void testCreatedStoreIsFunctional() {
        final BulkReplayJobStoreConfiguration configuration = createConfiguration();
        final BulkReplayJobStore store = configuration.bulkReplayJobStore();

        assertTrue(store.getAllJobs().isEmpty());
    }

    private BulkReplayJobStoreConfiguration createConfiguration() {
        final Properties props = new Properties();
        props.setProperty(NiFiProperties.BULK_REPLAY_MAX_JOBS, "100");
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, props);

        final BulkReplayJobStoreConfiguration config = new BulkReplayJobStoreConfiguration();
        config.setProperties(nifiProperties);
        return config;
    }
}
