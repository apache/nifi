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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class MockTransferFailureDestination implements TransferFailureDestination {
    private List<FlowFileRecord> flowFilesTransferred = new ArrayList<>();
    private List<String> swapFilesTransferred = new ArrayList<>();
    private final boolean rebalanceOnFailure;

    public MockTransferFailureDestination(final boolean rebalanceOnFailure) {
        this.rebalanceOnFailure = rebalanceOnFailure;
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles, final FlowFilePartitioner partitionerUsed) {
        flowFilesTransferred.addAll(flowFiles);
    }

    public List<FlowFileRecord> getFlowFilesTransferred() {
        return flowFilesTransferred;
    }

    @Override
    public void putAll(final Function<String, FlowFileQueueContents> queueContents, final FlowFilePartitioner partitionerUsed) {
        final FlowFileQueueContents contents = queueContents.apply("unit-test");
        flowFilesTransferred.addAll(contents.getActiveFlowFiles());
        swapFilesTransferred.addAll(contents.getSwapLocations());
    }

    @Override
    public boolean isRebalanceOnFailure(final FlowFilePartitioner partitionerUsed) {
        return rebalanceOnFailure;
    }

    public List<String> getSwapFilesTransferred() {
        return swapFilesTransferred;
    }
}
