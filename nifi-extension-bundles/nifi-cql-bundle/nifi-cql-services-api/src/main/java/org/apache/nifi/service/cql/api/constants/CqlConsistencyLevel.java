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

package org.apache.nifi.service.cql.api.constants;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;

/**
 * Allowable values for {@link CQLExecutionService#CONSISTENCY_LEVEL}: how many replicas must acknowledge a
 * read or write for it to succeed. Declared here, not taken from a driver enum, to keep this module
 * driver-independent; an implementation passes the value straight through, so these names must stay in sync
 * with what its driver parses ({@code CqlConsistencyLevelTest} / {@code ScyllaConsistencyLevelTest} in the
 * backend modules check that).
 */
public enum CqlConsistencyLevel implements DescribedValue {
    ANY("ANY", "ANY",
            "A write must be written to at least one node, or to a hinted handoff. Provides the lowest "
                    + "latency and the weakest guarantee. Write-only; not valid for reads."),
    ONE("ONE", "ONE",
            "Must be satisfied by the closest replica."),
    TWO("TWO", "TWO",
            "Must be satisfied by the two closest replicas."),
    THREE("THREE", "THREE",
            "Must be satisfied by the three closest replicas."),
    QUORUM("QUORUM", "QUORUM",
            "Must be satisfied by a quorum of replicas across all datacenters."),
    ALL("ALL", "ALL",
            "Must be satisfied by every replica. Provides the strongest guarantee, and fails if any replica "
                    + "is unavailable."),
    LOCAL_ONE("LOCAL_ONE", "LOCAL_ONE",
            "Must be satisfied by the closest replica in the local datacenter."),
    LOCAL_QUORUM("LOCAL_QUORUM", "LOCAL_QUORUM",
            "Must be satisfied by a quorum of replicas in the local datacenter, avoiding cross-datacenter "
                    + "latency."),
    EACH_QUORUM("EACH_QUORUM", "EACH_QUORUM",
            "Must be satisfied by a quorum of replicas in every datacenter. Write-only; not valid for reads."),
    SERIAL("SERIAL", "SERIAL",
            "Serial consistency for lightweight transactions, across all datacenters. Applies to the "
                    + "conditional phase of an LWT rather than to an ordinary read or write."),
    LOCAL_SERIAL("LOCAL_SERIAL", "LOCAL_SERIAL",
            "Serial consistency for lightweight transactions, restricted to the local datacenter.");

    private String value;
    private String displayName;
    private String description;

    CqlConsistencyLevel(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
