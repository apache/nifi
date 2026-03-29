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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractStateKeyDropIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStateKeyDropIT.class);
    private static final long RETRY_TIMEOUT_SECONDS = 30;
    private static final long RETRY_DELAY_MILLIS = 500;

    /**
     * Retrieves the state for a given processor, retrying on transient cluster errors.
     *
     * @param processorId the ID of the processor
     * @param scope       the scope of the state to retrieve (LOCAL or CLUSTER)
     * @return a map containing the state key-value pairs for the given scope
     * @throws NiFiClientException  NiFi Client Exception
     * @throws IOException          IO exception
     * @throws InterruptedException if the thread is interrupted while retrying
     */
    protected Map<String, String> getProcessorState(final String processorId, final Scope scope) throws NiFiClientException, IOException, InterruptedException {
        final long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(RETRY_TIMEOUT_SECONDS);

        while (true) {
            try {
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
            } catch (final NiFiClientException e) {
                if (!isTransientClusterError(e) || System.currentTimeMillis() > maxTime) {
                    throw e;
                }
                logger.info("Transient cluster error getting processor state for {}, retrying: {}", processorId, e.getMessage());
                Thread.sleep(RETRY_DELAY_MILLIS);
            }
        }
    }

    /**
     * Drops the state for a given processor, optionally setting a new state.
     *
     * @param processorId the ID of the processor
     * @param newState    a map containing the new state key-value pairs, or null to
     *                    drop the state without setting a new one
     * @return the response from the NiFi API
     * @throws NiFiClientException NiFi Client Exception
     * @throws IOException         IO exception
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
     * Runs a processor once and waits for it to stop, retrying on transient cluster errors.
     *
     * @param processor the processor entity to run
     * @throws NiFiClientException  if there is an error with the NiFi client
     * @throws IOException          if there is an IO error
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    protected void runProcessorOnce(final ProcessorEntity processor) throws NiFiClientException, IOException, InterruptedException {
        final long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(RETRY_TIMEOUT_SECONDS);

        while (true) {
            try {
                getNifiClient().getProcessorClient().runProcessorOnce(processor);
                break;
            } catch (final NiFiClientException e) {
                if (!isTransientClusterError(e) || System.currentTimeMillis() > maxTime) {
                    throw e;
                }
                logger.info("Transient cluster error running processor {} once, retrying: {}", processor.getId(), e.getMessage());
                Thread.sleep(RETRY_DELAY_MILLIS);
            }
        }

        getClientUtil().waitForStoppedProcessor(processor.getId());
    }

    private boolean isTransientClusterError(final NiFiClientException exception) {
        final String message = exception.getMessage();
        return message != null && message.contains("not connected");
    }
}
