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
package org.apache.nifi.controller.repository;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.claim.StandardContentClaimWriteCache;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.provenance.ProvenanceEventRepository;

import java.util.concurrent.atomic.AtomicLong;

public class StandardRepositoryContext extends AbstractRepositoryContext implements RepositoryContext {

    private final long maxAppendableClaimBytes;

    public StandardRepositoryContext(final Connectable connectable, final AtomicLong connectionIndex, final ContentRepository contentRepository, final FlowFileRepository flowFileRepository,
                                     final FlowFileEventRepository flowFileEventRepository, final CounterRepository counterRepository, final ProvenanceEventRepository provenanceRepository,
                                     final StateManager stateManager, final long maxAppendableClaimBytes) {
        super(connectable, connectionIndex, contentRepository, flowFileRepository, flowFileEventRepository, counterRepository, provenanceRepository, stateManager);
        this.maxAppendableClaimBytes = maxAppendableClaimBytes;
    }

    @Override
    public ContentClaimWriteCache createContentClaimWriteCache(final PerformanceTracker performanceTracker) {
        return new StandardContentClaimWriteCache(getContentRepository(), performanceTracker, maxAppendableClaimBytes, 8192);
    }
}
