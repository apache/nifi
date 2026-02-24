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

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestCachingConnectionStatusAnalyticsEngine extends TestStatusAnalyticsEngine {

    @Override
    public StatusAnalyticsEngine getStatusAnalyticsEngine(final FlowManager flowManager,
                                                          final StatusHistoryRepository componentStatusRepository, final StatusAnalyticsModelMapFactory statusAnalyticsModelMapFactory,
                                                          final long predictIntervalMillis, final long queryIntervalMillis, final String scoreName, final double scoreThreshold) {

        return new CachingConnectionStatusAnalyticsEngine(flowManager, componentStatusRepository, statusAnalyticsModelMapFactory, predictIntervalMillis,
                                                                                                queryIntervalMillis, scoreName, scoreThreshold);
    }

    @Test
    public void testCachedStatusAnalytics() {
        final StatusAnalyticsEngine statusAnalyticsEngine = new CachingConnectionStatusAnalyticsEngine(flowManager, statusRepository, statusAnalyticsModelMapFactory,
                                                                                                    DEFAULT_PREDICT_INTERVAL_MILLIS, DEFAULT_QUERY_INTERVAL_MILLIS,
                                                                                                    DEFAULT_SCORE_NAME, DEFAULT_SCORE_THRESHOLD);
        final StatusAnalytics statusAnalyticsA = statusAnalyticsEngine.getStatusAnalytics("A");
        final StatusAnalytics statusAnalyticsB = statusAnalyticsEngine.getStatusAnalytics("B");
        final StatusAnalytics statusAnalyticsTest = statusAnalyticsEngine.getStatusAnalytics("A");
        assertEquals(statusAnalyticsA, statusAnalyticsTest);
        assertNotEquals(statusAnalyticsB, statusAnalyticsTest);
    }

}
