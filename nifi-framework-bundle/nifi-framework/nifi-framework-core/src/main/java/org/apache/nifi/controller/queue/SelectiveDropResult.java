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

package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of a selective drop operation on a SwappablePriorityQueue.
 * Contains the dropped FlowFiles and information about swap file updates.
 */
public class SelectiveDropResult {

    private final List<FlowFileRecord> droppedFlowFiles;
    private final Map<String, String> swapLocationUpdates;

    /**
     * Creates a new SelectiveDropResult.
     *
     * @param droppedFlowFiles the FlowFiles that were dropped
     * @param swapLocationUpdates a map from old swap location to new swap location.
     *                            If the new location is null, the swap file was deleted entirely.
     */
    public SelectiveDropResult(final List<FlowFileRecord> droppedFlowFiles, final Map<String, String> swapLocationUpdates) {
        this.droppedFlowFiles = droppedFlowFiles;
        this.swapLocationUpdates = swapLocationUpdates;
    }

    /**
     * @return the FlowFiles that were dropped from the queue
     */
    public List<FlowFileRecord> getDroppedFlowFiles() {
        return Collections.unmodifiableList(droppedFlowFiles);
    }

    /**
     * @return a map from old swap location to new swap location. If the new location is null,
     *         all FlowFiles in that swap file matched the predicate and the file should be deleted.
     */
    public Map<String, String> getSwapLocationUpdates() {
        return Collections.unmodifiableMap(swapLocationUpdates);
    }

    /**
     * @return the total number of FlowFiles that were dropped
     */
    public int getDroppedCount() {
        return droppedFlowFiles.size();
    }

    /**
     * @return the total size in bytes of all dropped FlowFiles
     */
    public long getDroppedBytes() {
        return droppedFlowFiles.stream().mapToLong(FlowFileRecord::getSize).sum();
    }
}
