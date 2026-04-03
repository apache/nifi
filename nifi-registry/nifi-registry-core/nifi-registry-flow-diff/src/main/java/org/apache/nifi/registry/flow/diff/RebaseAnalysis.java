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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RebaseAnalysis {

    private final List<ClassifiedDifference> classifiedLocalChanges;
    private final Set<FlowDifference> upstreamDifferences;
    private final boolean rebaseAllowed;
    private final String analysisFingerprint;
    private final VersionedProcessGroup mergedSnapshot;

    public RebaseAnalysis(final List<ClassifiedDifference> classifiedLocalChanges, final Set<FlowDifference> upstreamDifferences,
                          final boolean rebaseAllowed, final String analysisFingerprint, final VersionedProcessGroup mergedSnapshot) {
        this.classifiedLocalChanges = Objects.requireNonNull(classifiedLocalChanges, "Classified local changes are required");
        this.upstreamDifferences = Objects.requireNonNull(upstreamDifferences, "Upstream differences are required");
        this.rebaseAllowed = rebaseAllowed;
        this.analysisFingerprint = Objects.requireNonNull(analysisFingerprint, "Analysis fingerprint is required");
        this.mergedSnapshot = mergedSnapshot;
    }

    public List<ClassifiedDifference> getClassifiedLocalChanges() {
        return classifiedLocalChanges;
    }

    public Set<FlowDifference> getUpstreamDifferences() {
        return upstreamDifferences;
    }

    public boolean isRebaseAllowed() {
        return rebaseAllowed;
    }

    public String getAnalysisFingerprint() {
        return analysisFingerprint;
    }

    public VersionedProcessGroup getMergedSnapshot() {
        return mergedSnapshot;
    }

    public static class ClassifiedDifference {
        private final FlowDifference difference;
        private final RebaseClassification classification;
        private final String conflictCode;
        private final String conflictDetail;

        public ClassifiedDifference(final FlowDifference difference, final RebaseClassification classification,
                                    final String conflictCode, final String conflictDetail) {
            this.difference = Objects.requireNonNull(difference, "Difference is required");
            this.classification = Objects.requireNonNull(classification, "Classification is required");
            this.conflictCode = conflictCode;
            this.conflictDetail = conflictDetail;
        }

        public static ClassifiedDifference compatible(final FlowDifference difference) {
            return new ClassifiedDifference(difference, RebaseClassification.COMPATIBLE, null, null);
        }

        public static ClassifiedDifference conflicting(final FlowDifference difference, final String conflictCode, final String conflictDetail) {
            return new ClassifiedDifference(difference, RebaseClassification.CONFLICTING, conflictCode, conflictDetail);
        }

        public static ClassifiedDifference unsupported(final FlowDifference difference, final String conflictCode, final String conflictDetail) {
            return new ClassifiedDifference(difference, RebaseClassification.UNSUPPORTED, conflictCode, conflictDetail);
        }

        public FlowDifference getDifference() {
            return difference;
        }

        public RebaseClassification getClassification() {
            return classification;
        }

        public String getConflictCode() {
            return conflictCode;
        }

        public String getConflictDetail() {
            return conflictDetail;
        }
    }
}
