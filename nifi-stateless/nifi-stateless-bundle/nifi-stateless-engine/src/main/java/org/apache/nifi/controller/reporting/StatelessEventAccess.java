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

package org.apache.nifi.controller.reporting;

import org.apache.nifi.action.Action;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsEngine;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.AbstractEventAccess;

import java.util.Collections;
import java.util.List;

public class StatelessEventAccess extends AbstractEventAccess {
    private final ProvenanceRepository provenanceRepository;

    public StatelessEventAccess(final ProcessScheduler processScheduler, final StatusAnalyticsEngine analyticsEngine, final FlowManager flowManager,
                                final FlowFileEventRepository flowFileEventRepository, final ProvenanceRepository provenanceRepository) {
        super(processScheduler, analyticsEngine, flowManager, flowFileEventRepository);
        this.provenanceRepository = provenanceRepository;
    }

    @Override
    public ProvenanceEventRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    @Override
    public List<Action> getFlowChanges(final int firstActionId, final int maxActions) {
        return Collections.emptyList();
    }
}
