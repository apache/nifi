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
package org.apache.nifi.web.dao;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.Set;

public interface ProcessorDAO {

    /**
     * @param id id
     * @return Determines if the specified processor is loaded
     */
    boolean hasProcessor(String id);

    /**
     * Verifies the specified processor can be created.
     *
     * @param processorDTO processor
     */
    void verifyCreate(ProcessorDTO processorDTO);

    /**
     * Creates a new Processor.
     *
     * @param groupId The group id where this component will be created
     * @param processorDTO The processor DTO
     * @return The new Processor
     */
    ProcessorNode createProcessor(String groupId, ProcessorDTO processorDTO);

    /**
     * Gets the Processor transfer object for the specified id.
     *
     * @param id Id of the processor to return
     * @return The Processor
     */
    ProcessorNode getProcessor(String id);

    /**
     * Gets all the Processor transfer objects for this controller.
     *
     * @param groupId group id
     * @param includeDescendants if processors from descendant groups should be included
     * @return List of all the Processors
     */
    Set<ProcessorNode> getProcessors(String groupId, boolean includeDescendants);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param processorDTO processor
     */
    void verifyUpdate(ProcessorDTO processorDTO);

    /**
     * Updates the configuration for the processor using the specified processorDTO.
     *
     * @param processorDTO processor
     * @return updated processor
     */
    ProcessorNode updateProcessor(ProcessorDTO processorDTO);

    /**
     * Verifies the specified processor can be removed.
     *
     * @param processorId processor id
     */
    void verifyDelete(String processorId);

    /**
     * Deletes the specified processor.
     *
     * @param processorId The processor id to delete
     */
    void deleteProcessor(String processorId);

    /**
     * Gets the specified processor.
     *
     * @param processorId processor id
     * @return state map
     */
    StateMap getState(String processorId, Scope scope);

    /**
     * Verifies the processor can clear state.
     *
     * @param processorId processor id
     */
    void verifyClearState(String processorId);

    /**
     * Clears the state of the specified processor.
     *
     * @param processorId processor id
     */
    void clearState(String processorId);
}
