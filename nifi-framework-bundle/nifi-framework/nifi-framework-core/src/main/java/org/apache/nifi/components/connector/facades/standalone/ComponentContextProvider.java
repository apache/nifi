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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;

import java.util.Map;

public interface ComponentContextProvider {
    ProcessContext createProcessContext(ProcessorNode processorNode, ParameterLookup parameterLookup);

    ProcessContext createProcessContext(ProcessorNode processorNode, Map<String, String> propertiesOverride, ParameterLookup parameterLookup);

    ValidationContext createValidationContext(ProcessorNode processorNode, Map<String, String> properties, ParameterLookup parameterLookup);

    ConfigurationContext createConfigurationContext(ControllerServiceNode serviceNode, ParameterLookup parameterLookup);

    ConfigurationContext createConfigurationContext(ControllerServiceNode serviceNode, Map<String, String> propertiesOverride, ParameterLookup parameterLookup);

    ValidationContext createValidationContext(ControllerServiceNode serviceNode, Map<String, String> properties, ParameterLookup parameterLookup);
}
