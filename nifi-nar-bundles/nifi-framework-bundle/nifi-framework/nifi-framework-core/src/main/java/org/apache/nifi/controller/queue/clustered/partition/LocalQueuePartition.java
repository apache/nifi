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

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.processor.FlowFileFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface LocalQueuePartition extends QueuePartition {
    boolean isActiveQueueEmpty();

    FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords);

    List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords);

    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    void acknowledge(FlowFileRecord flowFile);

    void acknowledge(Collection<FlowFileRecord> flowFiles);

    FlowFileRecord getFlowFile(final String flowFileUuid) throws IOException;

    List<FlowFileRecord> getListableFlowFiles();

    void inheritQueueContents(FlowFileQueueContents queueContents);

    QueueDiagnostics getQueueDiagnostics();
}
