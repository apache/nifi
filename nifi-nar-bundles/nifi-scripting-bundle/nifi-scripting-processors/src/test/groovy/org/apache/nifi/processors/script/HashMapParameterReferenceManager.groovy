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
package org.apache.nifi.processors.script

import org.apache.nifi.controller.ProcessorNode
import org.apache.nifi.controller.service.ControllerServiceNode
import org.apache.nifi.groups.ProcessGroup
import org.apache.nifi.parameter.ParameterContext
import org.apache.nifi.parameter.ParameterReferenceManager

class HashMapParameterReferenceManager implements ParameterReferenceManager {
    private final Map<String, ProcessorNode> processors = new HashMap<>()
    private final Map<String, ControllerServiceNode> controllerServices = new HashMap<>()

    @Override
    Set<ProcessorNode> getProcessorsReferencing(final ParameterContext parameterContext, final String parameterName) {
        final ProcessorNode node = processors.get(parameterName)
        return node == null ? Collections.emptySet() : Collections.singleton(node)
    }

    @Override
    Set<ControllerServiceNode> getControllerServicesReferencing(final ParameterContext parameterContext, final String parameterName) {
        final ControllerServiceNode node = controllerServices.get(parameterName)
        return node == null ? Collections.emptySet() : Collections.singleton(node)
    }

    @Override
    Set<ProcessGroup> getProcessGroupsBound(final ParameterContext parameterContext) {
        return Collections.emptySet()
    }

    void addProcessorReference(final String parameterName, final ProcessorNode processor) {
        processors.put(parameterName, processor)
    }

    void addControllerServiceReference(final String parameterName, final ControllerServiceNode service) {
        controllerServices.put(parameterName, service)
    }
}