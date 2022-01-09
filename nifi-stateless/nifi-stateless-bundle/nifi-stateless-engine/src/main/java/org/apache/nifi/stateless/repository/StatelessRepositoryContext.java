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

package org.apache.nifi.stateless.repository;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.AbstractRepositoryContext;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.provenance.ProvenanceEventRepository;

import java.util.concurrent.atomic.AtomicLong;

public class StatelessRepositoryContext extends AbstractRepositoryContext implements RepositoryContext {
    private final ContentClaimWriteCache writeCache;

    public StatelessRepositoryContext(final Connectable connectable, final AtomicLong connectionIndex, final ContentRepository contentRepository, final FlowFileRepository flowFileRepository,
                                      final FlowFileEventRepository flowFileEventRepository, final CounterRepository counterRepository, final ProvenanceEventRepository provenanceRepository,
                                      final StateManager stateManager) {
        super(connectable, connectionIndex, contentRepository, flowFileRepository, flowFileEventRepository, counterRepository, provenanceRepository, stateManager);
        writeCache = new StatelessContentClaimWriteCache(contentRepository);
    }

    @Override
    public ContentClaimWriteCache createContentClaimWriteCache() {
        return writeCache;
    }
}
