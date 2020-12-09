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
import org.apache.nifi.web.api.dto.ConnectionDTO;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

public class ConnectionSchemaFunction implements Function<ConnectionDTO, ConnectionSchema> {
    @Override
    public ConnectionSchema apply(ConnectionDTO connectionDTO) {
        Map<String, Object> map = new HashMap<>();
        map.put(ID_KEY, connectionDTO.getId());
        map.put(NAME_KEY, connectionDTO.getName());
        map.put(ConnectionSchema.SOURCE_ID_KEY, connectionDTO.getSource().getId());
        Set<String> selectedRelationships = nullToEmpty(connectionDTO.getSelectedRelationships());
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY, selectedRelationships.stream().sorted().collect(Collectors.toList()));
        map.put(ConnectionSchema.DESTINATION_ID_KEY, connectionDTO.getDestination().getId());

        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, connectionDTO.getBackPressureObjectThreshold());
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, connectionDTO.getBackPressureDataSizeThreshold());
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, connectionDTO.getFlowFileExpiration());
        List<String> queuePrioritizers = nullToEmpty(connectionDTO.getPrioritizers());
        if (queuePrioritizers.size() > 0) {
            map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, queuePrioritizers.get(0));
        }
        ConnectionSchema connectionSchema = new ConnectionSchema(map);
        if (ConnectableType.FUNNEL.name().equals(connectionDTO.getSource().getType())) {
            connectionSchema.addValidationIssue("Connection " + connectionDTO.getName() + " has type " + ConnectableType.FUNNEL.name() + " which is not supported by MiNiFi");
        }
        if (queuePrioritizers.size() > 1) {
            connectionSchema.addValidationIssue(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, CONNECTIONS_KEY, " has more than one queue prioritizer");
        }
        return connectionSchema;
    }
}
