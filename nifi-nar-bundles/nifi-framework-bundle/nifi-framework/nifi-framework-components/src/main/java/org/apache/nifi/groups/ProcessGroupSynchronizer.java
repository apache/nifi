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

import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

public interface ProcessGroupSynchronizer {

    /**
     * Synchronize the given Process Group to match the proposed snaphsot
     * @param group the Process Group to update
     * @param proposedFlow the proposed/desired state for the process group
     * @param synchronizationOptions options for how to synchronize the group
     */
    void synchronize(ProcessGroup group, VersionedExternalFlow proposedFlow, GroupSynchronizationOptions synchronizationOptions) throws ProcessorInstantiationException;

    void verifyCanSynchronize(ProcessGroup group, VersionedProcessGroup proposed, boolean verifyConnectionRemoval);

}
