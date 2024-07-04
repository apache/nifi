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
package org.apache.nifi.web.api.request;

import org.apache.nifi.prometheusutil.AbstractMetricsRegistry;
import org.apache.nifi.prometheusutil.BulletinMetricsRegistry;
import org.apache.nifi.prometheusutil.ClusterMetricsRegistry;
import org.apache.nifi.prometheusutil.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheusutil.JvmMetricsRegistry;
import org.apache.nifi.prometheusutil.NiFiMetricsRegistry;

/**
 * Flow Metrics Registries
 */
public enum FlowMetricsRegistry {
    NIFI("NIFI", NiFiMetricsRegistry.class),

    JVM("JVM", JvmMetricsRegistry.class),

    BULLETIN("BULLETIN", BulletinMetricsRegistry.class),

    CONNECTION("CONNECTION", ConnectionAnalyticsMetricsRegistry.class),

    CLUSTER("CLUSTER", ClusterMetricsRegistry.class);

    private final String registry;

    private final Class<? extends AbstractMetricsRegistry> registryClass;

    FlowMetricsRegistry(final String registry, final Class<? extends AbstractMetricsRegistry> registryClass) {
        this.registry = registry;
        this.registryClass = registryClass;
    }

    public String getRegistry() {
        return registry;
    }

    public Class<? extends AbstractMetricsRegistry> getRegistryClass() {
        return registryClass;
    }
}
