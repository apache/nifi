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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.web.api.dto.ConnectionDTO;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

public class ConnectionSchemaFunction implements Function<ConnectionDTO, ConnectionSchema> {
    @Override
    public ConnectionSchema apply(ConnectionDTO connectionDTO) {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, connectionDTO.getName());
        map.put(ConnectionSchema.SOURCE_NAME_KEY, connectionDTO.getSource().getName());
        Set<String> selectedRelationships = BaseSchema.nullToEmpty(connectionDTO.getSelectedRelationships());
        if (selectedRelationships.size() > 0) {
            map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAME_KEY, selectedRelationships.iterator().next());
        }
        map.put(ConnectionSchema.DESTINATION_NAME_KEY, connectionDTO.getDestination().getName());

        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, connectionDTO.getBackPressureObjectThreshold());
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, connectionDTO.getBackPressureDataSizeThreshold());
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, connectionDTO.getFlowFileExpiration());
        List<String> queuePrioritizers = BaseSchema.nullToEmpty(connectionDTO.getPrioritizers());
        if (queuePrioritizers.size() > 0) {
            map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, queuePrioritizers.get(0));
        }
        ConnectionSchema connectionSchema = new ConnectionSchema(map);
        if (ConnectableType.FUNNEL.name().equals(connectionDTO.getSource().getType())) {
            connectionSchema.validationIssues.add("Connection " + connectionDTO.getName() + " has type " + ConnectableType.FUNNEL.name() + " which is not supported by MiNiFi");
        }
        if (selectedRelationships.size() > 1) {
            connectionSchema.addValidationIssue(ConnectionSchema.SOURCE_RELATIONSHIP_NAME_KEY, CONNECTIONS_KEY, " has more than one selected relationship");
        }
        if (queuePrioritizers.size() > 1) {
            connectionSchema.addValidationIssue(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, CONNECTIONS_KEY, " has more than one queue prioritizer");
        }
        return connectionSchema;
    }
}
