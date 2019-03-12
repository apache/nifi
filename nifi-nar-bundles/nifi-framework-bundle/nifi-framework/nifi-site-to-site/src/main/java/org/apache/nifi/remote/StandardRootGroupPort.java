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
package org.apache.nifi.remote;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class StandardRootGroupPort extends AbstractPort implements RootGroupPort {

    private final Set<Relationship> relationships;

    public StandardRootGroupPort(final String id, final String name, final ProcessGroup processGroup,
            final TransferDirection direction, final ConnectableType type, final ProcessScheduler scheduler,
            final String yieldPeriod) {

        super(id, name, processGroup, type, scheduler);

        setScheduldingPeriod(MINIMUM_SCHEDULING_NANOS + " nanos");
        setYieldPeriod(yieldPeriod);

        relationships = direction == TransferDirection.RECEIVE ? Collections.singleton(AbstractPort.PORT_RELATIONSHIP) : Collections.emptySet();
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public boolean isTriggerWhenEmpty() {
        return true;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        getPublicPort().onTrigger(context, sessionFactory);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // nothing to do here -- we will never get called because we override onTrigger(ProcessContext, ProcessSessionFactory)
    }

    @Override
    public boolean isValid() {
        return getConnectableType() == ConnectableType.INPUT_PORT ? !getConnections(Relationship.ANONYMOUS).isEmpty() : true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final Collection<ValidationResult> validationErrors = new ArrayList<>();
        if (getScheduledState() == ScheduledState.STOPPED) {
            if (!isValid()) {
                final ValidationResult error = new ValidationResult.Builder()
                        .explanation(String.format("Output connection for port '%s' is not defined.", getName()))
                        .subject(String.format("Port '%s'", getName()))
                        .valid(false)
                        .build();
                validationErrors.add(error);
            }
        }
        return validationErrors;
    }

    @Override
    public void shutdown() {
        super.shutdown();

        getPublicPort().shutdown();
    }

    @Override
    public void onSchedulingStart() {
        super.onSchedulingStart();

        getPublicPort().start();
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }

    @Override
    public boolean isSideEffectFree() {
        return false;
    }

    @Override
    public String getComponentType() {
        return "RootGroupPort";
    }

}
