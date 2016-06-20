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

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConfigSchemaFunction implements Function<TemplateDTO, ConfigSchema> {
    private final FlowControllerSchemaFunction flowControllerSchemaFunction;
    private final ProcessorSchemaFunction processorSchemaFunction;
    private final ConnectionSchemaFunction connectionSchemaFunction;
    private final RemoteProcessingGroupSchemaFunction remoteProcessingGroupSchemaFunction;

    public ConfigSchemaFunction() {
        this(new FlowControllerSchemaFunction(), new ProcessorSchemaFunction(), new ConnectionSchemaFunction(), new RemoteProcessingGroupSchemaFunction(new RemoteInputPortSchemaFunction()));
    }

    public ConfigSchemaFunction(FlowControllerSchemaFunction flowControllerSchemaFunction, ProcessorSchemaFunction processorSchemaFunction,
                                ConnectionSchemaFunction connectionSchemaFunction, RemoteProcessingGroupSchemaFunction remoteProcessingGroupSchemaFunction) {
        this.flowControllerSchemaFunction = flowControllerSchemaFunction;
        this.processorSchemaFunction = processorSchemaFunction;
        this.connectionSchemaFunction = connectionSchemaFunction;
        this.remoteProcessingGroupSchemaFunction = remoteProcessingGroupSchemaFunction;
    }

    @Override
    public ConfigSchema apply(TemplateDTO templateDTO) {
        Map<String, Object> map = new HashMap<>();

        FlowSnippetDTO snippet = templateDTO.getSnippet();

        map.put(CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY, flowControllerSchemaFunction.apply(templateDTO).toMap());

        map.put(CommonPropertyKeys.PROCESSORS_KEY, BaseSchema.nullToEmpty(snippet.getProcessors()).stream()
                .map(processorSchemaFunction)
                .sorted(Comparator.comparing(ProcessorSchema::getName))
                .map(ProcessorSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.CONNECTIONS_KEY, BaseSchema.nullToEmpty(snippet.getConnections()).stream()
                .map(connectionSchemaFunction)
                .sorted(Comparator.comparing(ConnectionSchema::getName))
                .map(ConnectionSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.REMOTE_PROCESSING_GROUPS_KEY, BaseSchema.nullToEmpty(snippet.getRemoteProcessGroups()).stream()
                .map(remoteProcessingGroupSchemaFunction)
                .sorted(Comparator.comparing(RemoteProcessingGroupSchema::getName))
                .map(RemoteProcessingGroupSchema::toMap)
                .collect(Collectors.toList()));

        ConfigSchema configSchema = new ConfigSchema(map);
        return configSchema;
    }
}
