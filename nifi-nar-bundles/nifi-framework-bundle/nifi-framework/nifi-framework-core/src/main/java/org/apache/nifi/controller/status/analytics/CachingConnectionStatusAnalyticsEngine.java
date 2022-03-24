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
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
/**
 * <p>
 * An implementation of {@link StatusAnalyticsEngine} that supports caching of ConnectionStatusAnalytics objects.
 * Caching engine in use is an in-memory cache where the caching policy is to expire 30 minutes after initial write to cache.
 * </p>
 */
public class CachingConnectionStatusAnalyticsEngine extends ConnectionStatusAnalyticsEngine{
    private volatile Cache<String, StatusAnalytics> cache;
    private static final Logger LOG = LoggerFactory.getLogger(CachingConnectionStatusAnalyticsEngine.class);

    public CachingConnectionStatusAnalyticsEngine(FlowManager flowManager, StatusHistoryRepository statusRepository,
            StatusAnalyticsModelMapFactory statusAnalyticsModelMapFactory,
            long predictionIntervalMillis, long queryIntervalMillis, String scoreName, double scoreThreshold) {

        super(flowManager, statusRepository,  statusAnalyticsModelMapFactory, predictionIntervalMillis,
                           queryIntervalMillis, scoreName, scoreThreshold);
        this.cache = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build();
    }

    /**
     * Return a connection status analytics instance
     * @param identifier connection identifier
     * @return StatusAnalytics analytics object for given connection with id
     */
    @Override
    public StatusAnalytics getStatusAnalytics(String identifier) {

        StatusAnalytics connectionStatusAnalytics = cache.getIfPresent(identifier);
        if (connectionStatusAnalytics == null) {
            LOG.debug("Creating new status analytics object for connection id: {}", identifier);
            connectionStatusAnalytics = super.getStatusAnalytics(identifier);
            cache.put(identifier, connectionStatusAnalytics);
        }
        return connectionStatusAnalytics;

    }


}
