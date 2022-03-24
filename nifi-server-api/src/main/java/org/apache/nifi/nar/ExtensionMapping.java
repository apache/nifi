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

import org.apache.nifi.bundle.BundleCoordinate;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class ExtensionMapping {

    private final Map<String, Set<BundleCoordinate>> processorNames = new HashMap<>();
    private final Map<String, Set<BundleCoordinate>> controllerServiceNames = new HashMap<>();
    private final Map<String, Set<BundleCoordinate>> reportingTaskNames = new HashMap<>();

    private final BiFunction<Set<BundleCoordinate>, Set<BundleCoordinate>, Set<BundleCoordinate>> merger = (oldValue, newValue) -> {
        final Set<BundleCoordinate> merged = new HashSet<>();
        merged.addAll(oldValue);
        merged.addAll(newValue);
        return merged;
    };

    void addProcessor(final BundleCoordinate coordinate, final String processorName) {
        processorNames.computeIfAbsent(processorName, name -> new HashSet<>()).add(coordinate);
    }

    void addAllProcessors(final BundleCoordinate coordinate, final Collection<String> processorNames) {
        processorNames.forEach(name -> {
            addProcessor(coordinate, name);
        });
    }

    void addControllerService(final BundleCoordinate coordinate, final String controllerServiceName) {
        controllerServiceNames.computeIfAbsent(controllerServiceName, name -> new HashSet<>()).add(coordinate);
    }

    void addAllControllerServices(final BundleCoordinate coordinate, final Collection<String> controllerServiceNames) {
        controllerServiceNames.forEach(name -> {
            addControllerService(coordinate, name);
        });
    }

    void addReportingTask(final BundleCoordinate coordinate, final String reportingTaskName) {
        reportingTaskNames.computeIfAbsent(reportingTaskName, name -> new HashSet<>()).add(coordinate);
    }

    void addAllReportingTasks(final BundleCoordinate coordinate, final Collection<String> reportingTaskNames) {
        reportingTaskNames.forEach(name -> {
            addReportingTask(coordinate, name);
        });
    }

    void merge(final ExtensionMapping other) {
        other.getProcessorNames().forEach((name, otherCoordinates) -> {
            processorNames.merge(name, otherCoordinates, merger);
        });
        other.getControllerServiceNames().forEach((name, otherCoordinates) -> {
            controllerServiceNames.merge(name, otherCoordinates, merger);
        });
        other.getReportingTaskNames().forEach((name, otherCoordinates) -> {
            reportingTaskNames.merge(name, otherCoordinates, merger);
        });
    }

    public Map<String, Set<BundleCoordinate>> getProcessorNames() {
        return Collections.unmodifiableMap(processorNames);
    }

    public Map<String, Set<BundleCoordinate>> getControllerServiceNames() {
        return Collections.unmodifiableMap(controllerServiceNames);
    }

    public Map<String, Set<BundleCoordinate>> getReportingTaskNames() {
        return Collections.unmodifiableMap(reportingTaskNames);
    }

    public Map<String, Set<BundleCoordinate>> getAllExtensionNames() {
        final Map<String, Set<BundleCoordinate>> extensionNames = new HashMap<>();
        extensionNames.putAll(processorNames);
        extensionNames.putAll(controllerServiceNames);
        extensionNames.putAll(reportingTaskNames);
        return extensionNames;
    }

    public int size() {
        int size = 0;

        for (final Set<BundleCoordinate> coordinates : processorNames.values()) {
            size += coordinates.size();
        }
        for (final Set<BundleCoordinate> coordinates : controllerServiceNames.values()) {
            size += coordinates.size();
        }
        for (final Set<BundleCoordinate> coordinates : reportingTaskNames.values()) {
            size += coordinates.size();
        }

        return size;
    }

    public boolean isEmpty() {
        return processorNames.isEmpty() && controllerServiceNames.isEmpty() && reportingTaskNames.isEmpty();
    }
}
