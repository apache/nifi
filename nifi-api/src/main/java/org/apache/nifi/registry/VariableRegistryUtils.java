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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;

public class VariableRegistryUtils {


    public static VariableRegistry createVariableRegistry(){
        VariableRegistry variableRegistry = VariableRegistryFactory.getInstance();
        VariableRegistry envRegistry = VariableRegistryFactory.getInstance(System.getenv());
        VariableRegistry propRegistry = VariableRegistryFactory.getPropertiesInstance(System.getProperties());
        variableRegistry.addRegistry(envRegistry);
        variableRegistry.addRegistry(propRegistry);
        return variableRegistry;
    }

    public static VariableRegistry populateRegistry(VariableRegistry variableRegistry, final FlowFile flowFile, final Map<String, String> additionalAttributes){
        final Map<String, String> flowFileAttributes = flowFile == null ? null : flowFile.getAttributes();
        final Map<String, String> additionalMap = additionalAttributes == null ? null : additionalAttributes;

        Map<String, String> flowFileProps = null;
        if (flowFile != null) {
            flowFileProps = new HashMap<>();
            flowFileProps.put("flowFileId", String.valueOf(flowFile.getId()));
            flowFileProps.put("fileSize", String.valueOf(flowFile.getSize()));
            flowFileProps.put("entryDate", String.valueOf(flowFile.getEntryDate()));
            flowFileProps.put("lineageStartDate", String.valueOf(flowFile.getLineageStartDate()));
        }
        VariableRegistry newRegistry = VariableRegistryFactory.getInstance();
        newRegistry.addRegistry(variableRegistry);

        if(flowFileAttributes != null) {
            newRegistry.addRegistry(VariableRegistryFactory.getInstance(flowFileAttributes));
        }
        if(additionalMap != null) {
            newRegistry.addRegistry(VariableRegistryFactory.getInstance(additionalMap));
        }
        if(flowFileProps != null) {
            newRegistry.addRegistry(VariableRegistryFactory.getInstance(flowFileProps));
        }
        return newRegistry;
    }

}
