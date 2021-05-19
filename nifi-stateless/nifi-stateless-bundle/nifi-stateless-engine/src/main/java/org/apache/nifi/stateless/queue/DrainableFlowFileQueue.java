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

package org.apache.nifi.stateless.queue;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.List;

public interface DrainableFlowFileQueue extends FlowFileQueue {
    /**
     * Drains all of the FlowFiles from this FlowFileQueue into the given destination.
     * Note that any FlowFiles that have been consumed from the queue but not yet acknowledged
     * will remain so. Therefore, it is possible that the queue will not be empty after calling this
     * method, since there may be unacknowledged FlowFiles
     *
     * @param destination a List to drain the FlowFileRecords to
     */
    void drainTo(List<FlowFileRecord> destination);
}
