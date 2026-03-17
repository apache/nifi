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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link BulkReplayJobStore} bean based on {@code nifi.properties}.
 *
 * <p>Properties read:</p>
 * <ul>
 *   <li>{@code nifi.bulk.replay.max.jobs} — maximum number of job summaries retained in memory
 *       (default {@value NiFiProperties#DEFAULT_BULK_REPLAY_MAX_JOBS}). Oldest terminal jobs are evicted when the limit is exceeded.</li>
 * </ul>
 */
@Configuration
public class BulkReplayJobStoreConfiguration {

    private NiFiProperties properties;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Bean
    public BulkReplayJobStore bulkReplayJobStore() {
        final int maxJobs = properties.getBulkReplayMaxJobs();
        return new VolatileBulkReplayJobStore(maxJobs);
    }
}
