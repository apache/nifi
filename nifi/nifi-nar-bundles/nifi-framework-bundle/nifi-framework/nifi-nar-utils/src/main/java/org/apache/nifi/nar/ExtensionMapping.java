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
package org.apache.nifi.nar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ExtensionMapping {

    private final List<String> processorNames = new ArrayList<>();
    private final List<String> controllerServiceNames = new ArrayList<>();
    private final List<String> reportingTaskNames = new ArrayList<>();

    void addProcessor(final String processorName) {
        processorNames.add(processorName);
    }

    void addAllProcessors(final Collection<String> processorNames) {
        this.processorNames.addAll(processorNames);
    }

    void addControllerService(final String controllerServiceName) {
        controllerServiceNames.add(controllerServiceName);
    }

    void addAllControllerServices(final Collection<String> controllerServiceNames) {
        this.controllerServiceNames.addAll(controllerServiceNames);
    }

    void addReportingTask(final String reportingTaskName) {
        reportingTaskNames.add(reportingTaskName);
    }

    void addAllReportingTasks(final Collection<String> reportingTaskNames) {
        this.reportingTaskNames.addAll(reportingTaskNames);
    }

    public List<String> getProcessorNames() {
        return Collections.unmodifiableList(processorNames);
    }

    public List<String> getControllerServiceNames() {
        return Collections.unmodifiableList(controllerServiceNames);
    }

    public List<String> getReportingTaskNames() {
        return Collections.unmodifiableList(reportingTaskNames);
    }

    public List<String> getAllExtensionNames() {
        final List<String> extensionNames = new ArrayList<>();
        extensionNames.addAll(processorNames);
        extensionNames.addAll(controllerServiceNames);
        extensionNames.addAll(reportingTaskNames);
        return extensionNames;
    }
}
