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
package org.apache.nifi.controller.status.analytics;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CachingConnectionStatusAnalyticsEngine implements StatusAnalyticsEngine {
    private final ComponentStatusRepository statusRepository;
    private final FlowManager flowManager;
    private final FlowFileEventRepository flowFileEventRepository;
    private volatile Cache<String, ConnectionStatusAnalytics> cache;
    private static final Logger LOG = LoggerFactory.getLogger(CachingConnectionStatusAnalyticsEngine.class);

    public CachingConnectionStatusAnalyticsEngine(FlowManager flowManager, ComponentStatusRepository statusRepository, FlowFileEventRepository flowFileEventRepository) {
        this.flowManager = flowManager;
        this.statusRepository = statusRepository;
        this.flowFileEventRepository = flowFileEventRepository;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public StatusAnalytics getStatusAnalytics(String identifier) {

        ConnectionStatusAnalytics connectionStatusAnalytics = cache.getIfPresent(identifier);
        if (connectionStatusAnalytics == null) {
            LOG.debug("Creating new status analytics object for connection id: {}", identifier);
            connectionStatusAnalytics = new ConnectionStatusAnalytics(statusRepository, flowManager,flowFileEventRepository, identifier, true);
            connectionStatusAnalytics.init();
            cache.put(identifier, connectionStatusAnalytics);
        } else {
            LOG.debug("Pulled existing analytics from cache for connection id: {}", identifier);
            connectionStatusAnalytics.refresh();
        }
        return connectionStatusAnalytics;

    }


}
