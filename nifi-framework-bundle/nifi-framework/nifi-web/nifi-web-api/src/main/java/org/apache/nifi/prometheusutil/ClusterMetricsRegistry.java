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
package org.apache.nifi.prometheusutil;

import io.prometheus.client.Gauge;

/**
 * This registry contains metrics related to a NiFi cluster, such as connected node count and total node count
 */
public class ClusterMetricsRegistry extends AbstractMetricsRegistry {

    public ClusterMetricsRegistry() {

        nameToGaugeMap.put("IS_CLUSTERED",  Gauge.build()
                .name("cluster_is_clustered")
                .help("Whether this NiFi instance is clustered. Values are 0 or 1")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("IS_CONNECTED_TO_CLUSTER",  Gauge.build()
                .name("cluster_is_connected_to_cluster")
                .help("Whether this NiFi instance is connected to a cluster. Values are 0 or 1")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("CONNECTED_NODE_COUNT", Gauge.build()
                .name("cluster_connected_node_count")
                .help("The number of connected nodes in this cluster")
                .labelNames("instance", "connected_nodes")
                .register(registry));

        nameToGaugeMap.put("TOTAL_NODE_COUNT", Gauge.build()
                .name("cluster_total_node_count")
                .help("The total number of nodes in this cluster")
                .labelNames("instance")
                .register(registry));
    }
}
