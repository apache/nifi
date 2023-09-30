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
package org.apache.nifi.processors.opentelemetry.protocol;

/**
 * Enumeration of supported OpenTelemetry OTLP 1.0.0 Telemetry Types
 */
public enum TelemetryRequestType {
    LOGS("/v1/logs", "/opentelemetry.proto.collector.logs.v1.LogsService/Export"),

    METRICS("/v1/metrics", "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"),

    TRACES("/v1/traces", "/opentelemetry.proto.collector.trace.v1.TraceService/Export");

    private final String path;

    private final String grpcPath;

    TelemetryRequestType(final String path, final String grpcPath) {
        this.path = path;
        this.grpcPath = grpcPath;
    }

    /**
     * Get URI path for REST requests
     *
     * @return URI Pat
     */
    public String getPath() {
        return path;
    }

    /**
     * Get URI path for gRPC requests
     *
     * @return URI path for gRPC requests
     */
    public String getGrpcPath() {
        return grpcPath;
    }
}
