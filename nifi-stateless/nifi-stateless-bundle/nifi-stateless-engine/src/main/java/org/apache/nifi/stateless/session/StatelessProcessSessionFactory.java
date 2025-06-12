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

package org.apache.nifi.stateless.session;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;

public class StatelessProcessSessionFactory implements ProcessSessionFactory {
    private final Connectable connectable;
    private final RepositoryContextFactory contextFactory;
    private final ProvenanceEventRepository provenanceEventRepository;
    private final ProcessContextFactory processContextFactory;
    private final ExecutionProgress executionProgress;
    private final boolean requireSynchronousCommits;
    private final AsynchronousCommitTracker commitTracker;
    private final PerformanceTracker performanceTracker;

    public StatelessProcessSessionFactory(final Connectable connectable, final RepositoryContextFactory contextFactory,
                                          final ProvenanceEventRepository provenanceEventRepository, final ProcessContextFactory processContextFactory,
                                          final ExecutionProgress executionProgress, final boolean requireSynchronousCommits, final AsynchronousCommitTracker commitTracker,
                                          final PerformanceTracker performanceTracker) {
        this.connectable = connectable;
        this.contextFactory = contextFactory;
        this.provenanceEventRepository = provenanceEventRepository;
        this.processContextFactory = processContextFactory;
        this.executionProgress = executionProgress;
        this.requireSynchronousCommits = requireSynchronousCommits;
        this.commitTracker = commitTracker;
        this.performanceTracker = performanceTracker;
    }

    @Override
    public ProcessSession createSession() {
        final StatelessProcessSession session = new StatelessProcessSession(connectable, contextFactory, provenanceEventRepository, processContextFactory, executionProgress,
            requireSynchronousCommits, commitTracker, performanceTracker);
        executionProgress.registerCreatedSession(session);
        return session;
    }
}
