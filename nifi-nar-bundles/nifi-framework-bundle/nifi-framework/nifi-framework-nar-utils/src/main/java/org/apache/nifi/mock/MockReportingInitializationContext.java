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
package org.apache.nifi.mock;

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * A Mock ReportingInitializationContext that can be used to initialize a
 * ReportingTask for the purposes of documentation generation.
 *
 */
public class MockReportingInitializationContext implements ReportingInitializationContext {

    @Override
    public String getIdentifier() {
        return "mock-reporting-task";
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public long getSchedulingPeriod(TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return new MockControllerServiceLookup();
    }

    @Override
    public String getSchedulingPeriod() {
        return "";
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }

    @Override
    public ComponentLog getLogger() {
        return new MockComponentLogger();
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null;
    }
}
