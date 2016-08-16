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

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.scheduling.SchedulingStrategy;

/**
 * A ReportingConfiguration provides configuration information to a
 * ReportingTask at the time of initialization
 */
public interface ReportingInitializationContext extends KerberosContext {

    /**
     * @return the identifier for this ReportingTask
     */
    String getIdentifier();

    /**
     * @return the configured name for this ReportingTask
     */
    String getName();

    /**
     * Returns the amount of time, in the given {@link TimeUnit} that will
     * elapsed between the return of one execution of the
     * {@link ReportingTask}'s
     * {@link ReportingTask#onTrigger(ReportingContext) onTrigger} method and
     * the time at which the method is invoked again. This method will return
     * <code>-1L</code> if the Scheduling Strategy is not set to
     * {@link SchedulingStrategy#TIMER_DRIVEN}
     *
     * @param timeUnit unit of time for scheduling
     * @return period of time
     */
    long getSchedulingPeriod(TimeUnit timeUnit);

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * @return a String representation of the scheduling period
     */
    String getSchedulingPeriod();

    /**
     * @return the {@link SchedulingStrategy} that is used to trigger the task
     * to run
     */
    SchedulingStrategy getSchedulingStrategy();

    /**
     * @return a logger that can be used to log important events in a standard
     * way and generate bulletins when appropriate
     */
    ComponentLog getLogger();
}
