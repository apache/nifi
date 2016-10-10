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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReportingTaskEntityMerger implements ComponentEntityMerger<ReportingTaskEntity> {

    /**
     * Merges the ReportingTaskEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public void mergeComponents(final ReportingTaskEntity clientEntity, final Map<NodeIdentifier, ReportingTaskEntity> entityMap) {
        final ReportingTaskDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ReportingTaskDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ReportingTaskEntity> entry : entityMap.entrySet()) {
            final ReportingTaskEntity nodeReportingTaskEntity = entry.getValue();
            final ReportingTaskDTO nodeReportingTaskDto = nodeReportingTaskEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeReportingTaskDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final ReportingTaskDTO clientDto, final Map<NodeIdentifier, ReportingTaskDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

        int activeThreadCount = 0;
        for (final Map.Entry<NodeIdentifier, ReportingTaskDTO> nodeEntry : dtoMap.entrySet()) {
            final ReportingTaskDTO nodeReportingTask = nodeEntry.getValue();

            // consider the node reporting task if authorized
            if (nodeReportingTask != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();

                if (nodeReportingTask.getActiveThreadCount() != null) {
                    activeThreadCount += nodeReportingTask.getActiveThreadCount();
                }

                // merge the validation errors
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodeReportingTask.getValidationErrors());

                // aggregate the property descriptors
                nodeReportingTask.getDescriptors().values().stream().forEach(propertyDescriptor -> {
                    propertyDescriptorMap.computeIfAbsent(propertyDescriptor.getName(), nodeIdToPropertyDescriptor -> new HashMap<>()).put(nodeId, propertyDescriptor);
                });
            }
        }

        // merge property descriptors
        for (Map<NodeIdentifier, PropertyDescriptorDTO> propertyDescriptorByNodeId : propertyDescriptorMap.values()) {
            final Collection<PropertyDescriptorDTO> nodePropertyDescriptors = propertyDescriptorByNodeId.values();
            if (!nodePropertyDescriptors.isEmpty()) {
                // get the name of the property descriptor and find that descriptor being returned to the client
                final PropertyDescriptorDTO propertyDescriptor = nodePropertyDescriptors.iterator().next();
                final PropertyDescriptorDTO clientPropertyDescriptor = clientDto.getDescriptors().get(propertyDescriptor.getName());
                PropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, propertyDescriptorByNodeId);
            }
        }

        // set the merged active thread counts
        clientDto.setActiveThreadCount(activeThreadCount);

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
