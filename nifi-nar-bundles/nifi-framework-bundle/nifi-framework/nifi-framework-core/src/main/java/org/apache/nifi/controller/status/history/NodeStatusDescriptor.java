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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.NodeStatus;

import java.util.List;
import java.util.Objects;

public enum NodeStatusDescriptor {
    FREE_HEAP(
            "freeHeap",
            "Free Heap",
            "The amount of free memory in the heap that can be used by the Java virtual machine.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFreeHeap()),
    USED_HEAP(
            "usedHeap",
            "Used Heap",
            "The amount of used memory in the heap that is used by the Java virtual machine.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getUsedHeap()),
    HEAP_UTILIZATION(
            "heapUtilization",
            "Heap Utilization",
            "The percentage of available heap currently used by the Java virtual machine.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getHeapUtilization(),
            new ValueReducer<StatusSnapshot, Long>() {
                @Override
                public Long reduce(final List<StatusSnapshot> values) {
                    return (long) values.stream()
                            .map(snapshot -> snapshot.getStatusMetric(HEAP_UTILIZATION.getDescriptor()))
                            .filter(Objects::nonNull)
                            .mapToLong(value -> value)
                            .average()
                            .orElse(0L);
                }
            }),
    FREE_NON_HEAP(
            "freeNonHeap",
            "Free Non Heap",
            "The currently available non-heap memory that can be used by the Java virtual machine.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFreeNonHeap()),
    USED_NON_HEAP(
            "usedNonHeap",
            "Used Non Heap",
            "The current usage of non-heap memory that is used by the Java virtual machine.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getUsedNonHeap()),
    OPEN_FILE_HANDLES(
            "openFileHandles",
            "Open File Handles",
            "The current number of open file handles used by the Java virtual machine.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getOpenFileHandlers()),
    PROCESSOR_LOAD_AVERAGE(
            "processorLoadAverage",
            "Processor Load Average",
            "The processor load. Every measurement point represents the system load average for the last minute.",
            MetricDescriptor.Formatter.FRACTION,
            s -> Double.valueOf(s.getProcessorLoadAverage() * MetricDescriptor.FRACTION_MULTIPLIER).longValue(),
            new ValueReducer<StatusSnapshot, Long>() {
                @Override
                public Long reduce(final List<StatusSnapshot> values) {
                    return (long) values.stream()
                            .map(snapshot -> snapshot.getStatusMetric(HEAP_UTILIZATION.getDescriptor()))
                            .filter(Objects::nonNull)
                            .mapToLong(value -> value)
                            .average()
                            .orElse(0L);
                }
            }),
    TOTAL_THREADS(
            "totalThreads",
            "Number of total threads",
            "The current number of live threads in the Java virtual machine (both daemon and non-daemon threads).",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getTotalThreads()),
    EVENT_DRIVEN_THREADS(
            "eventDrivenThreads",
            "Number of event driven threads",
            "The current number of active threads in the event driven thread pool.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getEventDrivenThreads()),
    TIME_DRIVEN_THREADS(
            "timeDrivenThreads",
            "Number of time driven threads",
            "The current number of active threads in the time driven thread pool.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getTimerDrivenThreads()),
    FLOW_FILE_REPOSITORY_FREE_SPACE(
            "flowFileRepositoryFreeSpace",
            "Flow File Repository Free Space",
            "The usable space available for file repositories on the underlying storage mechanism",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFlowFileRepositoryFreeSpace()),
    FLOW_FILE_REPOSITORY_USED_SPACE(
            "flowFileRepositoryUsedSpace",
            "Flow File Repository Used Space",
            "The space in use on the underlying storage mechanism.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFlowFileRepositoryUsedSpace()),
    CONTENT_REPOSITORY_FREE_SPACE(
            "contentRepositoryFreeSpace",
            "Sum content Repository Free Space",
            "The usable space available for content repositories on the underlying storage mechanisms.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getContentRepositories().stream().mapToLong(r -> r.getFreeSpace()).sum()),
    CONTENT_REPOSITORY_USED_SPACE(
            "contentRepositoryUsedSpace",
            "Sum content Repository Used Space",
            "The space in use on the underlying storage mechanisms.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getContentRepositories().stream().mapToLong(r -> r.getUsedSpace()).sum()),
    PROVENANCE_REPOSITORY_FREE_SPACE(
            "provenanceRepositoryFreeSpace",
            "Sum provenance Repository Free Space",
            "The usable space available for use by the underlying storage mechanisms.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getProvenanceRepositories().stream().mapToLong(r -> r.getFreeSpace()).sum()),
    PROVENANCE_REPOSITORY_USED_SPACE(
            "provenanceRepositoryUsedSpace",
            "Sum provenance Repository Used Space",
            "The space in use on the underlying storage mechanisms.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getProvenanceRepositories().stream().mapToLong(r -> r.getUsedSpace()).sum());

    private final MetricDescriptor<NodeStatus> descriptor;

    NodeStatusDescriptor(
            final String field,
            final String label,
            final String description,
            final MetricDescriptor.Formatter formatter,
            final ValueMapper<NodeStatus> valueFunction) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
    }

    NodeStatusDescriptor(
            final String field,
            final String label,
            final String description,
            final MetricDescriptor.Formatter formatter,
            final ValueMapper<NodeStatus> valueFunction,
            final ValueReducer<StatusSnapshot, Long> reducer) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction, reducer);
    }


    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<NodeStatus> getDescriptor() {
        return descriptor;
    }
}
