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
import org.apache.nifi.web.api.entity.ConfigurationStepNamesEntity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigurationStepNamesEntityMerger {

    /**
     * Merges multiple ConfigurationStepNamesEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void merge(final ConfigurationStepNamesEntity clientEntity, final Map<NodeIdentifier, ConfigurationStepNamesEntity> entityMap) {
        if (clientEntity == null) {
            return;
        }

        final Set<String> allConfigurationStepNames = new HashSet<>();

        // Add client's configuration step names
        if (clientEntity.getConfigurationStepNames() != null) {
            allConfigurationStepNames.addAll(clientEntity.getConfigurationStepNames());
        }

        // Merge configuration step names from all nodes
        for (final Map.Entry<NodeIdentifier, ConfigurationStepNamesEntity> entry : entityMap.entrySet()) {
            final ConfigurationStepNamesEntity nodeEntity = entry.getValue();

            if (nodeEntity != null && nodeEntity.getConfigurationStepNames() != null) {
                allConfigurationStepNames.addAll(nodeEntity.getConfigurationStepNames());
            }
        }

        // Set the merged configuration step names (sorted for consistency)
        final List<String> sortedConfigurationStepNames = new ArrayList<>(allConfigurationStepNames);
        sortedConfigurationStepNames.sort(String::compareTo);
        clientEntity.setConfigurationStepNames(sortedConfigurationStepNames);
    }
}
