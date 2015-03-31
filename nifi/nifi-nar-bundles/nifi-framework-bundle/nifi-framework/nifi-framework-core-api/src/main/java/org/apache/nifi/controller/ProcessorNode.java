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
package org.apache.nifi.controller;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;

public abstract class ProcessorNode extends AbstractConfiguredComponent implements Connectable {

    public ProcessorNode(final Processor processor, final String id,
            final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider) {
        super(processor, id, validationContextFactory, serviceProvider);
    }

    public abstract boolean isIsolated();

    public abstract boolean isTriggerWhenAnyDestinationAvailable();

    @Override
    public abstract boolean isSideEffectFree();

    public abstract boolean isTriggeredSerially();

    public abstract boolean isEventDrivenSupported();

    public abstract boolean isHighThroughputSupported();

    @Override
    public abstract boolean isValid();

    public abstract void setScheduledState(ScheduledState scheduledState);

    public abstract void setBulletinLevel(LogLevel bulletinLevel);

    public abstract LogLevel getBulletinLevel();

    public abstract Processor getProcessor();

    public abstract void yield(long period, TimeUnit timeUnit);

    public abstract void setAutoTerminatedRelationships(Set<Relationship> relationships);

    public abstract Set<Relationship> getAutoTerminatedRelationships();

    public abstract void setSchedulingStrategy(SchedulingStrategy schedulingStrategy);

    @Override
    public abstract SchedulingStrategy getSchedulingStrategy();

    public abstract void setRunDuration(long duration, TimeUnit timeUnit);

    public abstract long getRunDuration(TimeUnit timeUnit);

    public abstract Map<String, String> getStyle();

    public abstract void setStyle(Map<String, String> style);

    /**
     * Returns the number of threads (concurrent tasks) currently being used by this Processor
     * @return
     */
    public abstract int getActiveThreadCount();
    
    /**
     * Verifies that this Processor can be started if the provided set of
     * services are enabled. This is introduced because we need to verify that all components
     * can be started before starting any of them. In order to do that, we need to know that this
     * component can be started if the given services are enabled, as we will then enable the given 
     * services before starting this component.
     * @param ignoredReferences
     */
    public abstract void verifyCanStart(Set<ControllerServiceNode> ignoredReferences);
}
