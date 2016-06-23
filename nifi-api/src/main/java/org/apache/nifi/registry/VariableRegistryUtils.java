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
package org.apache.nifi.registry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariableRegistryUtils {

    private final static Logger LOG =  LoggerFactory.getLogger(VariableRegistryUtils.class);

    public static VariableRegistry createSystemVariableRegistry(){
        VariableRegistry variableRegistry = VariableRegistryFactory.getInstance();
        VariableRegistry propRegistry = VariableRegistryFactory.getPropertiesInstance(System.getProperties());
        VariableRegistry envRegistry = VariableRegistryFactory.getInstance(System.getenv());
        variableRegistry.addRegistry(propRegistry);
        variableRegistry.addRegistry(envRegistry);
        return variableRegistry;
    }

    public static VariableRegistry createCustomVariableRegistry(Path[] properties){

        VariableRegistry customRegistry = null;
        try {
            customRegistry = VariableRegistryFactory.getPropertiesInstance(properties);
            customRegistry.addRegistry(createSystemVariableRegistry());
        } catch (IOException ioe){
            LOG.error("Exception thrown while attempting to add properties to registry",ioe);
        }
        return customRegistry;
    }

    public static VariableRegistry createFlowVariableRegistry(VariableRegistry variableRegistry, final FlowFile flowFile, final Map<String, String> additionalAttributes){
        final Map<String, String> flowFileAttributes = flowFile == null ? null : flowFile.getAttributes();
        final Map<String, String> additionalMap = additionalAttributes == null ? null : additionalAttributes;

        Map<String, String> flowFileProps = null;
        if (flowFile != null) {
            flowFileProps = new HashMap<>();
            flowFileProps.put("flowFileId", String.valueOf(flowFile.getId()));
            flowFileProps.put("fileSize", String.valueOf(flowFile.getSize()));
            flowFileProps.put("entryDate", String.valueOf(flowFile.getEntryDate()));
            flowFileProps.put("lineageStartDate", String.valueOf(flowFile.getLineageStartDate()));
            flowFileProps.put("lastQueueDate",String.valueOf(flowFile.getLastQueueDate()));
            flowFileProps.put("queueDateIndex",String.valueOf(flowFile.getQueueDateIndex()));
        }

        VariableRegistry newRegistry = VariableRegistryFactory.getInstance();

        if(flowFileAttributes != null) {
            newRegistry.addVariables(flowFileAttributes);
        }
        if(additionalMap != null) {
            newRegistry.addVariables(additionalMap);
        }
        if(flowFileProps != null) {
            newRegistry.addVariables(flowFileProps);
        }

        if(variableRegistry != null) {
            newRegistry.addRegistry(variableRegistry);
        }

        return newRegistry;
    }

}
