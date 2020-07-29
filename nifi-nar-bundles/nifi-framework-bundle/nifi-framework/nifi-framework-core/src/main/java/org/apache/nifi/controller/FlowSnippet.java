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
package org.apache.nifi.controller;

import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;

public interface FlowSnippet {
    /**
     * Validates that the FlowSnippet can be added to the given ProcessGroup
     * @param group the group to add the snippet to
     */
    void validate(final ProcessGroup group);

    /**
     * Verifies that the components referenced within the snippet are valid
     *
     * @throws IllegalStateException if any component within the snippet can is not a known extension
     */
    void verifyComponentTypesInSnippet();

    /**
     * Instantiates this snippet, adding it to the given Process Group
     *
     * @param flowManager the FlowManager
     * @param group the group to add the snippet to
     * @throws ProcessorInstantiationException if unable to instantiate any of the Processors within the snippet
     * @throws org.apache.nifi.controller.exception.ControllerServiceInstantiationException if unable to instantiate any of the Controller Services within the snippet
     */
    void instantiate(FlowManager flowManager, ProcessGroup group) throws ProcessorInstantiationException;
}

