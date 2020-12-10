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

package org.apache.nifi.minifi.commons.schema.v2;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.FunnelSchema;
import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.common.WritableSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONTROLLER_SERVICES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.FUNNELS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.OUTPUT_PORTS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROCESSORS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.REMOTE_PROCESS_GROUPS_KEY;

public class ProcessGroupSchemaV2 extends BaseSchemaWithIdAndName implements WritableSchema, ConvertableSchema<ProcessGroupSchema> {

    public static final String PROCESS_GROUPS_KEY = "Process Groups";
    public static final String ID_DEFAULT = "Root-Group";

    private String comment;
    private List<ProcessorSchema> processors;
    private List<FunnelSchema> funnels;
    private List<ConnectionSchema> connections;
    private List<RemoteProcessGroupSchemaV2> remoteProcessGroups;
    private List<ProcessGroupSchemaV2> processGroupSchemas;
    private List<PortSchema> inputPortSchemas;
    private List<PortSchema> outputPortSchemas;

    public ProcessGroupSchemaV2(Map map, String wrapperName) {
        super(map, wrapperName);

        processors = getOptionalKeyAsList(map, PROCESSORS_KEY, ProcessorSchema::new, wrapperName);
        funnels = getOptionalKeyAsList(map, FUNNELS_KEY, FunnelSchema::new, wrapperName);
        remoteProcessGroups = getOptionalKeyAsList(map, REMOTE_PROCESS_GROUPS_KEY, RemoteProcessGroupSchemaV2::new, wrapperName);
        connections = getOptionalKeyAsList(map, CONNECTIONS_KEY, ConnectionSchema::new, wrapperName);
        inputPortSchemas = getOptionalKeyAsList(map, INPUT_PORTS_KEY, m -> new PortSchema(m, "InputPort(id: {id}, name: {name})"), wrapperName);
        outputPortSchemas = getOptionalKeyAsList(map, OUTPUT_PORTS_KEY, m -> new PortSchema(m, "OutputPort(id: {id}, name: {name})"), wrapperName);
        processGroupSchemas = getOptionalKeyAsList(map, PROCESS_GROUPS_KEY, m -> new ProcessGroupSchemaV2(m, "ProcessGroup(id: {id}, name: {name})"), wrapperName);

        if (ConfigSchema.TOP_LEVEL_NAME.equals(wrapperName)) {
            if (inputPortSchemas.size() > 0) {
                addValidationIssue(INPUT_PORTS_KEY, wrapperName, "must be empty in root group as external input/output ports are currently unsupported");
            }
            if (outputPortSchemas.size() > 0) {
                addValidationIssue(OUTPUT_PORTS_KEY, wrapperName, "must be empty in root group as external input/output ports are currently unsupported");
            }
        } else if (ID_DEFAULT.equals(getId())) {
            addValidationIssue(ID_KEY, wrapperName, "must be set to a value not " + ID_DEFAULT + " if not in root group");
        }

        Set<String> portIds = getPortIds();
        connections.stream().filter(c -> portIds.contains(c.getSourceId())).forEachOrdered(c -> c.setNeedsSourceRelationships(false));


        Set<String> funnelIds = new HashSet<>(funnels.stream().map(FunnelSchema::getId).collect(Collectors.toList()));
        connections.stream().filter(c -> funnelIds.contains(c.getSourceId())).forEachOrdered(c -> c.setNeedsSourceRelationships(false));

        addIssuesIfNotNull(processors);
        addIssuesIfNotNull(remoteProcessGroups);
        addIssuesIfNotNull(processGroupSchemas);
        addIssuesIfNotNull(funnels);
        addIssuesIfNotNull(connections);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        String id = getId();
        if (!ID_DEFAULT.equals(id)) {
            result.put(ID_KEY, id);
        }
        StringUtil.doIfNotNullOrEmpty(getName(), name -> result.put(NAME_KEY, name));
        putListIfNotNull(result, PROCESSORS_KEY, processors);
        putListIfNotNull(result, PROCESS_GROUPS_KEY, processGroupSchemas);
        putListIfNotNull(result, INPUT_PORTS_KEY, inputPortSchemas);
        putListIfNotNull(result, OUTPUT_PORTS_KEY, outputPortSchemas);
        putListIfNotNull(result, FUNNELS_KEY, funnels);
        putListIfNotNull(result, CONNECTIONS_KEY, connections);
        putListIfNotNull(result, REMOTE_PROCESS_GROUPS_KEY, remoteProcessGroups.stream().map(RemoteProcessGroupSchemaV2::convert).collect(Collectors.toList()));
        return result;
    }

    public List<ProcessorSchema> getProcessors() {
        return processors;
    }

    public List<FunnelSchema> getFunnels() {
        return funnels;
    }

    public List<ConnectionSchema> getConnections() {
        return connections;
    }

    public List<RemoteProcessGroupSchemaV2> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public List<ProcessGroupSchemaV2> getProcessGroupSchemas() {
        return processGroupSchemas;
    }

    public Set<String> getPortIds() {
        Set<String> result = new HashSet<>();
        inputPortSchemas.stream().map(PortSchema::getId).forEachOrdered(result::add);
        outputPortSchemas.stream().map(PortSchema::getId).forEachOrdered(result::add);
        processGroupSchemas.stream().flatMap(p -> p.getPortIds().stream()).forEachOrdered(result::add);
        return result;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    protected String getId(Map map, String wrapperName) {
        return getOptionalKeyAsType(map, ID_KEY, String.class, wrapperName, ID_DEFAULT);
    }

    @Override
    public ProcessGroupSchema convert() {
        Map<String, Object> map = this.toMap();
        map.put(CONTROLLER_SERVICES_KEY, DEFAULT_PROPERTIES);
        return new ProcessGroupSchema(map, getWrapperName());
    }

    @Override
    public int getVersion() {
        return ConfigSchema.CONFIG_VERSION;
    }

    public List<PortSchema> getOutputPortSchemas() {
        return outputPortSchemas;
    }

    public List<PortSchema> getInputPortSchemas() {
        return inputPortSchemas;
    }

    @Override
    protected boolean isValidId(String value) {
        if (ID_DEFAULT.equals(value)) {
            return true;
        }
        return super.isValidId(value);
    }
}
