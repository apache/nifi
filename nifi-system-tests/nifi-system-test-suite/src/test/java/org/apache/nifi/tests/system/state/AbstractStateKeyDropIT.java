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
package org.apache.nifi.tests.system.state;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractStateKeyDropIT extends NiFiSystemIT {

    /**
     * Retrieves the state for a given processor.
     *
     * @param processorId the ID of the processor
     * @param scope       the scope of the state to retrieve (LOCAL or CLUSTER)
     * @return a map containing the cluster state key-value pairs
     * @throws IOException         IO exception
     * @throws NiFiClientException NiFi Client Exception
     */
    protected Map<String, String> getProcessorState(final String processorId, final Scope scope) throws NiFiClientException, IOException {
        final ComponentStateDTO componentState = getNifiClient().getProcessorClient().getProcessorState(processorId).getComponentState();
        final Map<String, String> state = new HashMap<>();

        switch (scope) {
            case LOCAL:
                if (componentState != null && componentState.getLocalState() != null && componentState.getLocalState().getState() != null) {
                    componentState.getLocalState().getState().forEach(entry -> state.put(entry.getKey(), entry.getValue()));
                }
                break;
            case CLUSTER:
                if (componentState != null && componentState.getClusterState() != null && componentState.getClusterState().getState() != null) {
                    componentState.getClusterState().getState().forEach(entry -> state.put(entry.getKey(), entry.getValue()));
                }
                break;
        }

        return state;
    }

    /**
     * Drops the state for a given processor, optionally setting a new state.
     *
     * @param processorId the ID of the processor
     * @param newState    a map containing the new state key-value pairs, or null to
     *                    drop the state without setting a new one
     * @return the response from the NiFi API
     * @throws IOException         IO exception
     * @throws NiFiClientException NiFi Client Exception
     */
    protected ComponentStateEntity dropProcessorState(final String processorId, final Map<String, String> newState) throws NiFiClientException, IOException {
        final ComponentStateEntity entity = new ComponentStateEntity();

        if (newState != null) {
            final ComponentStateDTO stateDto = new ComponentStateDTO();
            final StateMapDTO stateMapDTO = new StateMapDTO();
            final List<StateEntryDTO> entries = new ArrayList<>();
            newState.forEach((k, v) -> {
                final StateEntryDTO entry = new StateEntryDTO();
                entry.setKey(k);
                entry.setValue(v);
                entries.add(entry);
            });
            stateMapDTO.setState(entries);
            stateDto.setClusterState(stateMapDTO);

            entity.setComponentState(stateDto);
        }

        return getNifiClient().getProcessorClient().clearProcessorState(processorId, entity);
    }

    /**
     * Runs a processor once and waits for it to stop.
     *
     * @param processor the processor entity to run
     * @throws NiFiClientException  if there is an error with the NiFi client
     * @throws IOException          if there is an IO error
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    protected void runProcessorOnce(final ProcessorEntity processor) throws NiFiClientException, IOException, InterruptedException {
        getNifiClient().getProcessorClient().runProcessorOnce(processor);
        getClientUtil().waitForStoppedProcessor(processor.getId());
    }
}
