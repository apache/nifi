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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardProcessSession;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This task runs through all Connectable Components and goes through its incoming queues, polling for FlowFiles and accepting none. This causes the desired side effect of expiring old FlowFiles.
 */
public class ExpireFlowFiles implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ExpireFlowFiles.class);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;

    public ExpireFlowFiles(final FlowController flowController, final RepositoryContextFactory contextFactory) {
        this.flowController = flowController;
        this.contextFactory = contextFactory;
    }

    @Override
    public void run() {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        try {
            expireFlowFiles(rootGroup);
        } catch (final Exception e) {
            logger.error("Failed to expire FlowFiles due to {}", e.toString(), e);
        }
    }

    private StandardProcessSession createSession(final Connectable connectable) {
        final RepositoryContext context = contextFactory.newProcessContext(connectable, new AtomicLong(0L));
        final StandardProcessSessionFactory sessionFactory = new StandardProcessSessionFactory(context, () -> false);
        return sessionFactory.createSession();
    }

    private void expireFlowFiles(final Connectable connectable) {
        // determine if the incoming connections for this Connectable have Expiration configured.
        boolean expirationConfigured = false;
        for (final Connection incomingConn : connectable.getIncomingConnections()) {
            if (FormatUtils.getTimeDuration(incomingConn.getFlowFileQueue().getFlowFileExpiration(), TimeUnit.MILLISECONDS) > 0) {
                expirationConfigured = true;
                break;
            }
        }

        // If expiration is not configured... don't bother running through the FlowFileQueue
        if (!expirationConfigured) {
            return;
        }

        final StandardProcessSession session = createSession(connectable);
        session.expireFlowFiles();
        session.commit();
    }

    private void expireFlowFiles(final ProcessGroup group) {
        for (final ProcessorNode procNode : group.getProcessors()) {
            expireFlowFiles(procNode);
        }

        for (final Port port : group.getInputPorts()) {
            expireFlowFiles(port);
        }

        for (final Port port : group.getOutputPorts()) {
            expireFlowFiles(port);
        }

        for (final Funnel funnel : group.getFunnels()) {
            expireFlowFiles(funnel);
        }

        for (final RemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            for (final Port port : rpg.getInputPorts()) {
                expireFlowFiles(port);
            }

            for (final Port port : rpg.getOutputPorts()) {
                expireFlowFiles(port);
            }
        }

        for (final ProcessGroup child : group.getProcessGroups()) {
            expireFlowFiles(child);
        }
    }
}
