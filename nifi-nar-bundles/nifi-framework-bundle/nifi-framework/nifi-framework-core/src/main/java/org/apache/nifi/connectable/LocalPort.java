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
package org.apache.nifi.connectable;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides a mechanism by which <code>FlowFile</code>s can be transferred into and out of a <code>ProcessGroup</code> to and/or from another <code>ProcessGroup</code> within the same instance of
 * NiFi.
 */
public class LocalPort extends AbstractPort {

    // "_nifi.funnel.max.concurrent.tasks" is an experimental NiFi property allowing users to configure
    // the number of concurrent tasks to schedule for local ports and funnels.
    static final String MAX_CONCURRENT_TASKS_PROP_NAME = "_nifi.funnel.max.concurrent.tasks";

    // "_nifi.funnel.max.transferred.flowfiles" is an experimental NiFi property allowing users to configure
    // the maximum number of FlowFiles transferred each time a funnel or local port runs (rounded up to the nearest 1000).
    static final String MAX_TRANSFERRED_FLOWFILES_PROP_NAME = "_nifi.funnel.max.transferred.flowfiles";

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    final int maxIterations;

    public LocalPort(final String id, final String name, final ConnectableType type, final ProcessScheduler scheduler, final NiFiProperties nifiProperties) {
        super(id, name, null, type, scheduler);

        int maxConcurrentTasks = Integer.parseInt(nifiProperties.getProperty(MAX_CONCURRENT_TASKS_PROP_NAME, "1"));
        setMaxConcurrentTasks(maxConcurrentTasks);

        int maxTransferredFlowFiles = Integer.parseInt(nifiProperties.getProperty(MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "10000"));
        maxIterations = Math.max(1, (int) Math.ceil(maxTransferredFlowFiles / 1000.0));
    }

    @Override
    public boolean isValid() {
        return !getConnections(Relationship.ANONYMOUS).isEmpty() && hasIncomingConnection();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final Collection<ValidationResult> validationErrors = new ArrayList<>();
        if (getConnections(Relationship.ANONYMOUS).isEmpty()) {
            validationErrors.add(new ValidationResult.Builder()
                .explanation("Port has no outgoing connections")
                .subject(String.format("Port '%s'", getName()))
                .valid(false)
                .build());
        }

        if (!hasIncomingConnection()) {
            validationErrors.add(new ValidationResult.Builder()
                .explanation("Port has no incoming connections")
                .subject(String.format("Port '%s'", getName()))
                .valid(false)
                .build());
        }

        return validationErrors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        readLock.lock();
        try {
            Set<Relationship> available = context.getAvailableRelationships();
            int iterations = 0;
            while (!available.isEmpty()) {
                final List<FlowFile> flowFiles = session.get(1000);
                if (flowFiles.isEmpty()) {
                    break;
                }

                session.transfer(flowFiles, Relationship.ANONYMOUS);
                session.commit();

                // If there are fewer than 1,000 FlowFiles available to transfer, or if we
                // have hit the configured FlowFile cap, we want to stop. This prevents us from
                // holding the Timer-Driven Thread for an excessive amount of time.
                if (flowFiles.size() < 1000 || ++iterations >= maxIterations) {
                    break;
                }

                available = context.getAvailableRelationships();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void updateConnection(final Connection connection) throws IllegalStateException {
        writeLock.lock();
        try {
            super.updateConnection(connection);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void addConnection(final Connection connection) throws IllegalArgumentException {
        writeLock.lock();
        try {
            super.addConnection(connection);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeConnection(final Connection connection) throws IllegalArgumentException, IllegalStateException {
        writeLock.lock();
        try {
            super.removeConnection(connection);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<Connection> getConnections() {
        readLock.lock();
        try {
            return super.getConnections();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Connection> getConnections(Relationship relationship) {
        readLock.lock();
        try {
            return super.getConnections(relationship);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<Connection> getIncomingConnections() {
        readLock.lock();
        try {
            return super.getIncomingConnections();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean hasIncomingConnection() {
        readLock.lock();
        try {
            return super.hasIncomingConnection();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isTriggerWhenEmpty() {
        return false;
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }

    @Override
    public boolean isSideEffectFree() {
        return true;
    }

    @Override
    public String getComponentType() {
        return "Local Port";
    }
}
