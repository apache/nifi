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

import java.util.Set;

/**
 * Computes a rebase analysis by classifying local changes against upstream changes and, when all local changes are
 * compatible, producing a merged snapshot that overlays the compatible local changes onto the target version.
 */
public interface RebaseEngine {

    /**
     * Classify the local changes against the upstream changes for a rebase onto the given target version, without
     * modifying the target snapshot. Use this for read-only analysis; the returned analysis has a {@code null} merged
     * snapshot.
     *
     * @param localDifferences    the differences between the current version and the local flow
     * @param upstreamDifferences the differences between the current version and the target version
     * @param targetSnapshot      the target version snapshot; it is only read, never modified
     * @return the rebase analysis with classified local changes and fingerprint, and a {@code null} merged snapshot
     */
    RebaseAnalysis classify(Set<FlowDifference> localDifferences, Set<FlowDifference> upstreamDifferences, VersionedProcessGroup targetSnapshot);

    /**
     * Analyze the local changes against the upstream changes for a rebase onto the given target version and, when the
     * rebase is allowed, build the merged snapshot.
     *
     * @param localDifferences    the differences between the current version and the local flow
     * @param upstreamDifferences the differences between the current version and the target version
     * @param targetSnapshot      the target version snapshot. When the rebase is allowed, this snapshot is mutated in
     *                            place to become the merged snapshot, so callers must pass a snapshot that is safe to
     *                            modify (i.e. not a snapshot that must remain a clean copy of the target version).
     * @return the rebase analysis, including the classified local changes and, when allowed, the merged snapshot
     */
    RebaseAnalysis analyze(Set<FlowDifference> localDifferences, Set<FlowDifference> upstreamDifferences, VersionedProcessGroup targetSnapshot);
}
