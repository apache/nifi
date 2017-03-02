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

import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessorInitializationContext;

public abstract class AbstractReportingTask extends AbstractConfigurableComponent implements ReportingTask {

    private String identifier;
    private String name;
    private long schedulingNanos;
    private ControllerServiceLookup serviceLookup;
    private ComponentLog logger;

    @Override
    public final void initialize(final ReportingInitializationContext config) throws InitializationException {
        identifier = config.getIdentifier();
        logger = config.getLogger();
        name = config.getName();
        schedulingNanos = config.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        serviceLookup = config.getControllerServiceLookup();

        init(config);
    }

    /**
     * @return the {@link ControllerServiceLookup} that was passed to the
     * {@link #init(ProcessorInitializationContext)} method
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    /**
     * @return the identifier of this Reporting Task
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return the name of this Reporting Task
     */
    protected String getName() {
        return name;
    }

    /**
     * @param timeUnit of scheduling period
     * @return the amount of times that elapses between the moment that this
     * ReportingTask finishes its invocation of
     * {@link #onTrigger(ReportingContext)} and the next time that
     * {@link #onTrigger(ReportingContext)} is called.
     */
    protected long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Provides a mechanism by which subclasses can perform initialization of
     * the Reporting Task before it is scheduled to be run
     *
     * @param config context
     * @throws InitializationException if failure to init
     */
    protected void init(final ReportingInitializationContext config) throws InitializationException {
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its initialize method
     */
    protected ComponentLog getLogger() {
        return logger;
    }
}
