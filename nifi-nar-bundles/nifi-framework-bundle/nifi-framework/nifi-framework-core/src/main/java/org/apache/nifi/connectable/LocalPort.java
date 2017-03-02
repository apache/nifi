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

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;

/**
 * Provides a mechanism by which <code>FlowFile</code>s can be transferred into and out of a <code>ProcessGroup</code> to and/or from another <code>ProcessGroup</code> within the same instance of
 * NiFi.
 */
public class LocalPort extends AbstractPort {

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public LocalPort(final String id, final String name, final ProcessGroup processGroup, final ConnectableType type, final ProcessScheduler scheduler) {
        super(id, name, processGroup, type, scheduler);
    }

    @Override
    public boolean isValid() {
        return !getConnections(Relationship.ANONYMOUS).isEmpty();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final Collection<ValidationResult> validationErrors = new ArrayList<>();
        if (!isValid()) {
            final ValidationResult error = new ValidationResult.Builder()
                    .explanation(String.format("Output connection for port '%s' is not defined.", getName()))
                    .subject(String.format("Port '%s'", getName()))
                    .valid(false)
                    .build();
            validationErrors.add(error);
        }
        return validationErrors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        readLock.lock();
        try {
            final List<FlowFile> flowFiles = session.get(100);
            if (!flowFiles.isEmpty()) {
                session.transfer(flowFiles, Relationship.ANONYMOUS);
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
