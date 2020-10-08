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
package org.apache.nifi.reporting;

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.metrics.EmptyFlowFileEvent;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.provenance.ProvenanceRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class StandardEventAccess extends AbstractEventAccess implements UserAwareEventAccess {
    private final FlowManager flowManager;
    private final FlowFileEventRepository flowFileEventRepository;
    private final Authorizer authorizer;
    private final ProvenanceRepository provenanceRepository;
    private final AuditService auditService;

    public StandardEventAccess(final FlowManager flowManager, final FlowFileEventRepository flowFileEventRepository, final ProcessScheduler processScheduler,
                               final Authorizer authorizer, final ProvenanceRepository provenanceRepository, final AuditService auditService, final StatusAnalyticsEngine statusAnalyticsEngine) {
        super(processScheduler, statusAnalyticsEngine, flowManager, flowFileEventRepository);
        this.flowFileEventRepository = flowFileEventRepository;
        this.flowManager = flowManager;
        this.authorizer = authorizer;
        this.provenanceRepository = provenanceRepository;
        this.auditService = auditService;
    }


    @Override
    public List<Action> getFlowChanges(final int firstActionId, final int maxActions) {
        final History history = auditService.getActions(firstActionId, maxActions);
        return new ArrayList<>(history.getActions());
    }

    @Override
    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    @Override
    public ProcessorStatus getProcessorStatus(final String processorId, final NiFiUser user) {
        final ProcessorNode procNode = flowManager.getProcessorNode(processorId);
        if (procNode == null) {
            return null;
        }

        FlowFileEvent flowFileEvent = flowFileEventRepository.reportTransferEvents(processorId, System.currentTimeMillis());
        if (flowFileEvent == null) {
            flowFileEvent = EmptyFlowFileEvent.INSTANCE;
        }

        final Predicate<Authorizable> authorizer = authorizable -> authorizable.isAuthorized(this.authorizer, RequestAction.READ, user);
        return getProcessorStatus(flowFileEvent, procNode, authorizer);
    }

    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final NiFiUser user, final int recursiveStatusDepth) {
        final RepositoryStatusReport repoStatusReport = generateRepositoryStatusReport();
        return getGroupStatus(groupId, repoStatusReport, user, recursiveStatusDepth);
    }


    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport, final NiFiUser user) {
        final ProcessGroup group = flowManager.getGroup(groupId);

        // on demand status request for a specific user... require authorization per component and filter results as appropriate
        return getGroupStatus(group, statusReport, authorizable -> authorizable.isAuthorized(this.authorizer, RequestAction.READ, user), Integer.MAX_VALUE, 1);
    }

    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final NiFiUser user) {
        final RepositoryStatusReport repoStatusReport = generateRepositoryStatusReport();
        return getGroupStatus(groupId, repoStatusReport, user);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @param recursiveStatusDepth the number of levels deep we should recurse and still include the the processors' statuses, the groups' statuses, etc. in the returned ProcessGroupStatus
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport, final NiFiUser user, final int recursiveStatusDepth) {
        final ProcessGroup group = flowManager.getGroup(groupId);

        // on demand status request for a specific user... require authorization per component and filter results as appropriate
        return getGroupStatus(group, statusReport, authorizable -> authorizable.isAuthorized(this.authorizer, RequestAction.READ, user), recursiveStatusDepth, 1);
    }
}
