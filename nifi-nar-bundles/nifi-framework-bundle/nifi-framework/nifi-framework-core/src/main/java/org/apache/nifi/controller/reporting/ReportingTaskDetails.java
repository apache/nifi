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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingTask;

/**
 * Holder for StandardReportingTaskNode to atomically swap out the component.
 */
class ReportingTaskDetails {

    private final ReportingTask reportingTask;
    private final ComponentLog componentLog;
    private final BundleCoordinate bundleCoordinate;

    public ReportingTaskDetails(final LoggableComponent<ReportingTask> reportingTask) {
        this.reportingTask = reportingTask.getComponent();
        this.componentLog = reportingTask.getLogger();
        this.bundleCoordinate = reportingTask.getBundleCoordinate();
    }

    public ReportingTask getReportingTask() {
        return reportingTask;
    }

    public ComponentLog getComponentLog() {
        return componentLog;
    }

    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }
}
