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

import java.util.List;

public class FlowFileQueueContents {
    private final List<String> swapLocations;
    private final List<FlowFileRecord> activeFlowFiles;
    private final QueueSize swapSize;

    public FlowFileQueueContents(final List<FlowFileRecord> activeFlowFiles, final List<String> swapLocations, final QueueSize swapSize) {
        this.activeFlowFiles = activeFlowFiles;
        this.swapLocations = swapLocations;
        this.swapSize = swapSize;
    }

    public List<FlowFileRecord> getActiveFlowFiles() {
        return activeFlowFiles;
    }

    public List<String> getSwapLocations() {
        return swapLocations;
    }

    public QueueSize getSwapSize() {
        return swapSize;
    }
}
