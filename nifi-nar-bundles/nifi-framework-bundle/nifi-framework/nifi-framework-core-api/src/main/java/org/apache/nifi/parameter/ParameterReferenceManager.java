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
package org.apache.nifi.parameter;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;

import java.util.Collections;
import java.util.Set;

/**
 * A component that is responsible for tracking which components reference each Parameter
 */
public interface ParameterReferenceManager {

    /**
     * Returns the set of all Processors in the flow that reference the parameter with the given name
     *
     * @param  parameterContext the Parameter Context that the parameter belongs to
     * @param parameterName the name of the parameter
     * @return the set of all Processors in the flow that reference the parameter with the given name
     */
    Set<ProcessorNode> getProcessorsReferencing(ParameterContext parameterContext, String parameterName);

    /**
     * Returns the set of all ControllerServices in the flow that reference the parameter with the given name
     *
     * @param  parameterContext the Parameter Context that the parameter belongs to
     * @param parameterName the name of the parameter
     * @return the set of all ControllerServices in the flow that reference the parameter with the given name
     */
    Set<ControllerServiceNode> getControllerServicesReferencing(ParameterContext parameterContext, String parameterName);

    /**
     * Returns the set of all Process Groups that are bound to the given Parameter Context
     * @param parameterContext the Parameter Context
     * @return the set of all Process Groups that are bound to the given Parameter Context
     */
    Set<ProcessGroup> getProcessGroupsBound(ParameterContext parameterContext);

    ParameterReferenceManager EMPTY = new ParameterReferenceManager() {
        @Override
        public Set<ProcessorNode> getProcessorsReferencing(final ParameterContext parameterContext, final String parameterName) {
            return Collections.emptySet();
        }

        @Override
        public Set<ControllerServiceNode> getControllerServicesReferencing(final ParameterContext parameterContext, final String parameterName) {
            return Collections.emptySet();
        }

        @Override
        public Set<ProcessGroup> getProcessGroupsBound(final ParameterContext parameterContext) {
            return Collections.emptySet();
        }
    };
}
