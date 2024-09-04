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
package org.apache.nifi.cluster.coordination.http.replication;

/**
 * Enumeration of HTTP headers for Request Replication with lowercasing for compatibility with HTTP/2
 */
public enum RequestReplicationHeader {
    /**
     * Indicator to cancel transaction processing
     */
    CANCEL_TRANSACTION("cancel-transaction"),

    /**
     * Seed for deterministic cluster identifier generation
     */
    CLUSTER_ID_GENERATION_SEED("cluster-id-generation-seed"),

    /**
     * Indicator to continue transaction processing
     */
    EXECUTION_CONTINUE("execution-continue"),

    /**
     * When replicating a request to the cluster coordinator, it may be useful to denote that the request should
     * be replicated only to a single node. This happens, for instance, when retrieving a Provenance Event that
     * we know lives on a specific node. This request must still be replicated through the cluster coordinator.
     * This header tells the cluster coordinator the UUID's (comma-separated list, possibly with spaces between)
     * of the nodes that the request should be replicated to.
     */
    REPLICATION_TARGET_ID("replication-target-id"),

    /**
     * When we replicate a request across the cluster, we replicate it only from the cluster coordinator.
     * If the request needs to be replicated by another node, it first replicates the request to the coordinator,
     * which then replicates the request on the node's behalf. This header name and value are used to denote
     * that the request has already been to the cluster coordinator, and the cluster coordinator is the one replicating
     * the request. This allows us to know that the request should be serviced, rather than proxied back to the
     * cluster coordinator.
     */
    REQUEST_REPLICATED("request-replicated"),

    /**
     * Transaction Identifier for replicated requests
     */
    REQUEST_TRANSACTION_ID("request-transaction-id"),

    /**
     * The HTTP header that the requestor specifies to ask a node if they are able to process a given request.
     * The value is always 202-Accepted. The node will respond with 202 ACCEPTED if it is able to
     * process the request, 417 EXPECTATION_FAILED otherwise.
     */
    VALIDATION_EXPECTS("validation-expects");

    private final String header;

    RequestReplicationHeader(final String header) {
        this.header = header;
    }

    public String getHeader() {
        return header;
    }
}
