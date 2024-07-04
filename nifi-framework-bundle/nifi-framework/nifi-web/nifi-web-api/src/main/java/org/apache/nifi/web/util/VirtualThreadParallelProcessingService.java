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
package org.apache.nifi.web.util;

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.status.analytics.StatusAnalytics;
import org.apache.nifi.prometheusutil.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheusutil.PrometheusMetricsUtil;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.controller.ControllerFacade;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VirtualThreadParallelProcessingService implements PredictionBasedParallelProcessingService, Closeable {
    private boolean analyticsEnabled;
    private ExecutorService parallelProcessingExecutorService;
    private long parallelProcessingTimeout;

    public VirtualThreadParallelProcessingService(final NiFiProperties properties) {
        // We need to make processing timeout shorter than the web request timeout as if they overlap Jetty may throw IllegalStateException
        parallelProcessingTimeout = Math.round(FormatUtils.getPreciseTimeDuration(
                properties.getProperty(NiFiProperties.WEB_REQUEST_TIMEOUT, "1 min"), TimeUnit.MILLISECONDS)) - 5000;

        analyticsEnabled = Boolean.parseBoolean(
                properties.getProperty(NiFiProperties.ANALYTICS_PREDICTION_ENABLED, Boolean.FALSE.toString()));

        if (analyticsEnabled) {
            this.parallelProcessingExecutorService = Executors.newVirtualThreadPerTaskExecutor();
        }
    }

    @Override
    public Collection<Map<String, Long>> createConnectionStatusAnalyticsMetricsAndCollectPredictions(
            ControllerFacade controllerFacade, ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry, String instanceId) {

        Collection<Map<String, Long>> predictions = Collections.synchronizedList(new ArrayList<>());

        if (!analyticsEnabled) {
            return predictions;
        }

        final Set<Connection> connections = controllerFacade.getFlowManager().findAllConnections();
        final CountDownLatch countDownLatch = new CountDownLatch(connections.size());
        try {
            for (Connection c : connections) {
                parallelProcessingExecutorService.execute(() -> {
                    try {
                        final StatusAnalytics statusAnalytics = controllerFacade.getConnectionStatusAnalytics(c.getIdentifier());
                        PrometheusMetricsUtil.createConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry,
                                statusAnalytics,
                                instanceId,
                                "Connection",
                                c.getName(),
                                c.getIdentifier(),
                                c.getProcessGroup().getIdentifier(),
                                c.getSource().getName(),
                                c.getSource().getIdentifier(),
                                c.getDestination().getName(),
                                c.getDestination().getIdentifier()
                        );
                        predictions.add(statusAnalytics.getPredictions());
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        } finally {
            try {
                boolean finished = countDownLatch.await(parallelProcessingTimeout, TimeUnit.MILLISECONDS);
                if (!finished) {
                    throw new WebApplicationException("Populating flow metrics timed out");
                }
            } catch (InterruptedException e) {
                throw new WebApplicationException("Populating flow metrics cancelled");
            }
        }
        return predictions;
    }

    @Override
    public void close() throws IOException {
        if (parallelProcessingExecutorService != null) {
            parallelProcessingExecutorService.close();
        }
    }
}
