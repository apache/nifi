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

package org.apache.nifi.groups;

import java.util.Set;

import org.apache.nifi.registry.flow.diff.FlowDifference;

public class VersionControlFields {
    private volatile boolean locallyModified;
    private volatile boolean stale;
    private volatile String syncFailureExplanation = "Not yet synchronized with Flow Registry";
    private volatile Set<FlowDifference> flowDifferences;

    boolean isLocallyModified() {
        return locallyModified;
    }

    void setLocallyModified(final boolean locallyModified) {
        this.locallyModified = locallyModified;
    }

    boolean isStale() {
        return stale;
    }

    void setStale(final boolean stale) {
        this.stale = stale;
    }

    String getSyncFailureExplanation() {
        return syncFailureExplanation;
    }

    void setSyncFailureExplanation(final String syncFailureExplanation) {
        this.syncFailureExplanation = syncFailureExplanation;
    }

    Set<FlowDifference> getFlowDifferences() {
        return flowDifferences;
    }

    void setFlowDifferences(final Set<FlowDifference> flowDifferences) {
        this.flowDifferences = flowDifferences;
    }
}
