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
package org.apache.nifi.web.api.metrics;

import io.prometheus.client.CollectorRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * Prometheus Metrics Writer
 */
public interface PrometheusMetricsWriter {
    /**
     * Write collection of metrics registries to provided stream
     *
     * @param registries Collector Registries
     * @param outputStream Output Stream
     * @throws IOException Thrown on failure to write metrics
     */
    void write(Collection<CollectorRegistry> registries, OutputStream outputStream) throws IOException;
}
