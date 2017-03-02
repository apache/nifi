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
package org.apache.nifi.controller.scheduling;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRepository;

public class ProcessContextFactory {

    private final ContentRepository contentRepo;
    private final FlowFileRepository flowFileRepo;
    private final FlowFileEventRepository flowFileEventRepo;
    private final CounterRepository counterRepo;
    private final ProvenanceEventRepository provenanceRepo;

    public ProcessContextFactory(final ContentRepository contentRepository, final FlowFileRepository flowFileRepository,
            final FlowFileEventRepository flowFileEventRepository, final CounterRepository counterRepository,
            final ProvenanceEventRepository provenanceRepository) {

        this.contentRepo = contentRepository;
        this.flowFileRepo = flowFileRepository;
        this.flowFileEventRepo = flowFileEventRepository;
        this.counterRepo = counterRepository;
        this.provenanceRepo = provenanceRepository;
    }

    public ProcessContext newProcessContext(final Connectable connectable, final AtomicLong connectionIndex) {
        return new ProcessContext(connectable, connectionIndex, contentRepo, flowFileRepo, flowFileEventRepo, counterRepo, provenanceRepo);
    }
}
