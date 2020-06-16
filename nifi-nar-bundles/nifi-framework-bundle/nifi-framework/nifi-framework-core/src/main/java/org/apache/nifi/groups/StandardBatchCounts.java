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

package org.apache.nifi.groups;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StandardBatchCounts implements BatchCounts {
    private static final Logger logger = LoggerFactory.getLogger(StandardBatchCounts.class);

    private final ProcessGroup processGroup;
    private Map<String, Integer> counts = null;
    private boolean hasBeenReset = false;
    private final StateManager stateManager;

    private Map<String, Integer> restoredValues;

    public StandardBatchCounts(final ProcessGroup processGroup, final StateManager stateManager) {
        this.processGroup = processGroup;
        this.stateManager = stateManager;
    }

    @Override
    public synchronized void reset() {
        counts = null;
        hasBeenReset = true;

        try {
            stateManager.clear(Scope.LOCAL);
        } catch (final Exception e) {
            logger.error("Failed to update local state for {}. This could result in the batch.output.* attributes being inaccurate if NiFi is restarted before this is resolved", processGroup, e);
        }
    }

    @Override
    public synchronized Map<String, Integer> captureCounts() {
        // If reset() hasn't been called, we don't want to provide any counts. This can happen if the Process Group's
        // FlowFileConcurrency and/or FlowFileOutboundPolicy is changed while data is already being processed in the Process Group.
        // In this case, we don't want to provide counts until we've cleared all those FlowFiles out, and when that happens, reset() will be called.
        if (!hasBeenReset) {
            return restoreState();
        }

        if (counts == null) {
            counts = new HashMap<>();
            final Map<String, String> stateMapValues = new HashMap<>();

            for (final Port outputPort : processGroup.getOutputPorts()) {
                int count = 0;

                for (final Connection connection : outputPort.getIncomingConnections()) {
                    count += connection.getFlowFileQueue().size().getObjectCount();
                }

                final String name = outputPort.getName();
                counts.put(name, count);
                stateMapValues.put(name, String.valueOf(count));
            }

            try {
                stateManager.setState(stateMapValues, Scope.LOCAL);
            } catch (final Exception e) {
                logger.error("Failed to update local state for {}. This could result in the batch.output.* attributes being inaccurate if NiFi is restarted before this is resolved", processGroup, e);
            }
        }

        return counts;
    }

    private Map<String, Integer> restoreState() {
        if (restoredValues != null) {
            return restoredValues;
        }

        final StateMap stateMap;
        try {
            stateMap = stateManager.getState(Scope.LOCAL);
        } catch (IOException e) {
            logger.error("Failed to restore local state for {}. This could result in the batch.output.* attributes being inaccurate for the current batch of FlowFiles", processGroup, e);
            return Collections.emptyMap();
        }

        restoredValues = new HashMap<>();

        final Map<String, String> rawValues = stateMap.toMap();
        rawValues.forEach((k, v) -> restoredValues.put(k, Integer.parseInt(v)));
        return restoredValues;
    }
}
