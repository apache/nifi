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

import org.apache.nifi.web.api.dto.status.StatusDescriptorDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StatusHistoryUtil {

    public static StatusHistoryDTO createStatusHistoryDTO(final StatusHistory statusHistory) {
        final List<StatusSnapshotDTO> snapshotDtos = new ArrayList<>();
        final Set<MetricDescriptor<?>> metricDescriptors = new LinkedHashSet<>();
        final Map<String, String> componentDetails = new LinkedHashMap<>(statusHistory.getComponentDetails());

        final Set<String> metricNames = new HashSet<>();
        for (final StatusSnapshot snapshot : statusHistory.getStatusSnapshots()) {
            final StatusSnapshotDTO snapshotDto = createStatusSnapshotDto(snapshot);
            snapshotDtos.add(snapshotDto);
            metricNames.addAll(snapshotDto.getStatusMetrics().keySet());
            metricDescriptors.addAll(snapshot.getMetricDescriptors());
        }

        // We need to ensure that the 'aggregate snapshot' has an entry for every metric, including counters.
        // So for any metric that has is not in the aggregate snapshot, add it with a value of 0
        for (final StatusSnapshotDTO snapshotDto : snapshotDtos) {
            final Map<String, Long> metrics = snapshotDto.getStatusMetrics();
            for (final String metricName : metricNames) {
                if (!metrics.containsKey(metricName)) {
                    metrics.put(metricName, 0L);
                }
            }
        }

        final StatusHistoryDTO dto = new StatusHistoryDTO();
        dto.setGenerated(new Date());
        dto.setComponentDetails(componentDetails);
        dto.setFieldDescriptors(createFieldDescriptorDtos(metricDescriptors));
        dto.setAggregateSnapshots(snapshotDtos);
        return dto;
    }

    public static StatusDescriptorDTO createStatusDescriptorDto(final MetricDescriptor<?> metricDescriptor) {
        final StatusDescriptorDTO dto = new StatusDescriptorDTO();
        dto.setDescription(metricDescriptor.getDescription());
        dto.setField(metricDescriptor.getField());
        dto.setFormatter(metricDescriptor.getFormatter().name());
        dto.setLabel(metricDescriptor.getLabel());
        return dto;
    }

    public static List<StatusDescriptorDTO> createFieldDescriptorDtos(final Collection<MetricDescriptor<?>> metricDescriptors) {
        final StatusDescriptorDTO[] standardMetricDescriptors = new StatusDescriptorDTO[metricDescriptors.size()];
        final List<StatusDescriptorDTO> counterMetricDescriptors = new ArrayList<>();

        for (final MetricDescriptor<?> metricDescriptor : metricDescriptors) {
            if (metricDescriptor instanceof StandardMetricDescriptor) {
                standardMetricDescriptors[metricDescriptor.getMetricIdentifier()] = createStatusDescriptorDto(metricDescriptor);
            } else if (metricDescriptor instanceof CounterMetricDescriptor) {
                counterMetricDescriptors.add(createStatusDescriptorDto(metricDescriptor));
            } else {
                throw new IllegalArgumentException("Unknown metric descriptor type: " + metricDescriptor.getClass().getName());
            }
        }

        // Ordered standard metric descriptors are added first, then counter metric descriptors in lexicographical order of their label.
        counterMetricDescriptors.sort(Comparator.comparing(StatusDescriptorDTO::getLabel));
        final List<StatusDescriptorDTO> result = new ArrayList<>(metricDescriptors.size());
        result.addAll(Arrays.stream(standardMetricDescriptors).filter(Objects::nonNull).toList());
        result.addAll(counterMetricDescriptors);
        return result;
    }

    public static List<StatusDescriptorDTO> createFieldDescriptorDtos(final StatusHistory statusHistory) {
        final List<StatusDescriptorDTO> dtos = new ArrayList<>();

        final Set<MetricDescriptor<?>> allDescriptors = new LinkedHashSet<>();
        for (final StatusSnapshot statusSnapshot : statusHistory.getStatusSnapshots()) {
            allDescriptors.addAll(statusSnapshot.getMetricDescriptors());
        }

        for (final MetricDescriptor<?> metricDescriptor : allDescriptors) {
            dtos.add(createStatusDescriptorDto(metricDescriptor));
        }

        return dtos;
    }

    public static StatusSnapshotDTO createStatusSnapshotDto(final StatusSnapshot statusSnapshot) {
        final StatusSnapshotDTO dto = new StatusSnapshotDTO();

        dto.setTimestamp(statusSnapshot.getTimestamp());
        final Map<String, Long> statusMetrics = new HashMap<>();
        for (final MetricDescriptor<?> descriptor : statusSnapshot.getMetricDescriptors()) {
            statusMetrics.put(descriptor.getField(), statusSnapshot.getStatusMetric(descriptor));
        }
        dto.setStatusMetrics(statusMetrics);

        return dto;
    }

}
