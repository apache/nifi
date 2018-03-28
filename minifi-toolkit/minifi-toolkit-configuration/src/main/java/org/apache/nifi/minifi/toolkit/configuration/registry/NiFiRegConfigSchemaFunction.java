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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ControllerServiceSchema;
import org.apache.nifi.minifi.commons.schema.FunnelSchema;
import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.CollectionUtil;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.OUTPUT_PORTS_KEY;

public class NiFiRegConfigSchemaFunction implements Function<VersionedFlowSnapshot, ConfigSchema> {

    private final NiFiRegFlowControllerSchemaFunction flowControllerSchemaFunction;
    private final NiFiRegProcessorSchemaFunction processorSchemaFunction;
    private final NiFiRegControllerServiceSchemaFunction controllerServiceSchemaFunction;
    private final NiFiRegConnectionSchemaFunction connectionSchemaFunction;
    private final NiFiRegFunnelSchemaFunction funnelSchemaFunction;
    private final NiFiRegRemoteProcessGroupSchemaFunction remoteProcessGroupSchemaFunction;
    private final NiFiRegPortSchemaFunction inputPortSchemaFunction;
    private final NiFiRegPortSchemaFunction outputPortSchemaFunction;

    public NiFiRegConfigSchemaFunction() {
        this(
            new NiFiRegFlowControllerSchemaFunction(),
            new NiFiRegProcessorSchemaFunction(),
            new NiFiRegControllerServiceSchemaFunction(),
            new NiFiRegConnectionSchemaFunction(),
            new NiFiRegFunnelSchemaFunction(),
            new NiFiRegRemoteProcessGroupSchemaFunction(new NiFiRegRemotePortSchemaFunction()),
            new NiFiRegPortSchemaFunction(INPUT_PORTS_KEY),
            new NiFiRegPortSchemaFunction(OUTPUT_PORTS_KEY)
        );
    }

    public NiFiRegConfigSchemaFunction(final NiFiRegFlowControllerSchemaFunction flowControllerSchemaFunction,
                                       final NiFiRegProcessorSchemaFunction processorSchemaFunction,
                                       final NiFiRegControllerServiceSchemaFunction controllerServiceSchemaFunction,
                                       final NiFiRegConnectionSchemaFunction connectionSchemaFunction,
                                       final NiFiRegFunnelSchemaFunction funnelSchemaFunction,
                                       final NiFiRegRemoteProcessGroupSchemaFunction remoteProcessGroupSchemaFunction,
                                       final NiFiRegPortSchemaFunction inputPortSchemaFunction,
                                       final NiFiRegPortSchemaFunction outputPortSchemaFunction) {
        this.flowControllerSchemaFunction = flowControllerSchemaFunction;
        this.processorSchemaFunction = processorSchemaFunction;
        this.controllerServiceSchemaFunction = controllerServiceSchemaFunction;
        this.connectionSchemaFunction = connectionSchemaFunction;
        this.funnelSchemaFunction = funnelSchemaFunction;
        this.remoteProcessGroupSchemaFunction = remoteProcessGroupSchemaFunction;
        this.inputPortSchemaFunction = inputPortSchemaFunction;
        this.outputPortSchemaFunction = outputPortSchemaFunction;
    }

    @Override
    public ConfigSchema apply(final VersionedFlowSnapshot versionedFlowSnapshot) {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.FLOW_CONTROLLER_PROPS_KEY, flowControllerSchemaFunction.apply(versionedFlowSnapshot).toMap());

        VersionedProcessGroup versionedProcessGroup = versionedFlowSnapshot.getFlowContents();
        addVersionedProcessGroup(map, versionedProcessGroup);

        return new ConfigSchema(map);
    }

    protected void addVersionedProcessGroup(Map<String, Object> map, VersionedProcessGroup versionedProcessGroup) {
        addVersionedProcessGroup(map, null, null, versionedProcessGroup);
    }

    protected Map<String, Object> addVersionedProcessGroup(Map<String, Object> map, String id, String name, VersionedProcessGroup versionedProcessGroup) {
        if (!StringUtil.isNullOrEmpty(id)) {
            map.put(ID_KEY, id);
        }

        if (!StringUtil.isNullOrEmpty(name)) {
            map.put(NAME_KEY, name);
        }

        map.put(CommonPropertyKeys.PROCESSORS_KEY, nullToEmpty(versionedProcessGroup.getProcessors()).stream()
                .map(processorSchemaFunction)
                .sorted(Comparator.comparing(ProcessorSchema::getName))
                .map(ProcessorSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.CONTROLLER_SERVICES_KEY, nullToEmpty(versionedProcessGroup.getControllerServices()).stream()
                .map(controllerServiceSchemaFunction)
                .sorted(Comparator.comparing(ControllerServiceSchema::getName))
                .map(ControllerServiceSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.CONNECTIONS_KEY, nullToEmpty(versionedProcessGroup.getConnections()).stream()
                .map(connectionSchemaFunction)
                .sorted(Comparator.comparing(ConnectionSchema::getName))
                .map(ConnectionSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.FUNNELS_KEY, CollectionUtil.nullToEmpty(versionedProcessGroup.getFunnels()).stream()
                .map(funnelSchemaFunction)
                .sorted(Comparator.comparing(FunnelSchema::getId))
                .map(FunnelSchema::toMap)
                .collect(Collectors.toList()));

        map.put(CommonPropertyKeys.REMOTE_PROCESS_GROUPS_KEY, nullToEmpty(versionedProcessGroup.getRemoteProcessGroups()).stream()
                .map(remoteProcessGroupSchemaFunction)
                .sorted(Comparator.comparing(RemoteProcessGroupSchema::getName))
                .map(RemoteProcessGroupSchema::toMap)
                .collect(Collectors.toList()));

        map.put(INPUT_PORTS_KEY, nullToEmpty(versionedProcessGroup.getInputPorts()).stream()
                .map(inputPortSchemaFunction)
                .sorted(Comparator.comparing(PortSchema::getName))
                .map(PortSchema::toMap)
                .collect(Collectors.toList()));

        map.put(OUTPUT_PORTS_KEY, nullToEmpty(versionedProcessGroup.getOutputPorts()).stream()
                .map(outputPortSchemaFunction)
                .sorted(Comparator.comparing(PortSchema::getName))
                .map(PortSchema::toMap)
                .collect(Collectors.toList()));

        map.put(ProcessGroupSchema.PROCESS_GROUPS_KEY, nullToEmpty(versionedProcessGroup.getProcessGroups()).stream()
                .map(p -> addVersionedProcessGroup(new HashMap<>(), p.getIdentifier(), p.getName(), p)).collect(Collectors.toList()));

        return map;
    }
}
