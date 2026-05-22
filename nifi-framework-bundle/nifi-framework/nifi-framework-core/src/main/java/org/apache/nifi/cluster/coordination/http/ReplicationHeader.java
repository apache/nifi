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
package org.apache.nifi.cluster.coordination.http;

/**
 * Enumeration of HTTP headers for Cluster Request Replication with lowercasing for compatibility with HTTP/2
 */
public enum ReplicationHeader {
    /** Boolean indicator that the Cluster Coordinator is initiating the replicated request to other nodes */
    REQUEST_REPLICATED("request-replicated");

    private final String header;

    ReplicationHeader(final String header) {
        this.header = header;
    }

    public String getHeader() {
        return header;
    }
}
