/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ParentGroupIdResolver {
    private final Map<String, String> processorIdToParentIdMap;
    private final Map<String, String> inputPortIdToParentIdMap;
    private final Map<String, String> outputPortIdToParentIdMap;
    private final Map<String, String> remoteInputPortIdToParentIdMap;

    public ParentGroupIdResolver(ProcessGroupSchema processGroupSchema) {
        this.processorIdToParentIdMap = getParentIdMap(processGroupSchema, ProcessGroupSchema::getProcessors);
        this.inputPortIdToParentIdMap = getParentIdMap(processGroupSchema, ProcessGroupSchema::getInputPortSchemas);
        this.outputPortIdToParentIdMap = getParentIdMap(processGroupSchema, ProcessGroupSchema::getOutputPortSchemas);
        this.remoteInputPortIdToParentIdMap = getRemoteInputPortParentIdMap(processGroupSchema);
    }

    protected static Map<String, String> getParentIdMap(ProcessGroupSchema processGroupSchema, Function<ProcessGroupSchema, Collection<? extends BaseSchemaWithIdAndName>> schemaAccessor) {
        Map<String, String> map = new HashMap<>();
        getParentIdMap(processGroupSchema, map, schemaAccessor);
        return map;
    }

    protected static void getParentIdMap(ProcessGroupSchema processGroupSchema, Map<String, String> output, Function<ProcessGroupSchema,
            Collection<? extends BaseSchemaWithIdAndName>> schemaAccessor) {
        schemaAccessor.apply(processGroupSchema).forEach(p -> output.put(p.getId(), processGroupSchema.getId()));
        processGroupSchema.getProcessGroupSchemas().forEach(p -> getParentIdMap(p, output, schemaAccessor));
    }

    protected static Map<String, String> getRemoteInputPortParentIdMap(ProcessGroupSchema processGroupSchema) {
        Map<String, String> result = new HashMap<>();
        getRemoteInputPortParentIdMap(processGroupSchema, result);
        return result;
    }

    protected static void getRemoteInputPortParentIdMap(ProcessGroupSchema processGroupSchema, Map<String, String> output) {
        for (RemoteProcessingGroupSchema remoteProcessingGroupSchema : processGroupSchema.getRemoteProcessingGroups()) {
            for (RemoteInputPortSchema remoteInputPortSchema : remoteProcessingGroupSchema.getInputPorts()) {
                output.put(remoteInputPortSchema.getId(), remoteProcessingGroupSchema.getName());
            }
        }
        processGroupSchema.getProcessGroupSchemas().forEach(p -> getRemoteInputPortParentIdMap(p, output));
    }

    public String getRemoteInputPortParentId(String id) {
        return remoteInputPortIdToParentIdMap.get(id);
    }

    public String getInputPortParentId(String id) {
        return inputPortIdToParentIdMap.get(id);
    }

    public String getOutputPortParentId(String id) {
        return outputPortIdToParentIdMap.get(id);
    }

    public String getProcessorParentId(String id) {
        return processorIdToParentIdMap.get(id);
    }
}
