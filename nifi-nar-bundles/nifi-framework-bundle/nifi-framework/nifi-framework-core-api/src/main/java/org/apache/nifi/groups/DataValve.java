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

/**
 * <p>
 *   A DataValve regulates the flow of FlowFiles between Process Groups.
 * </p>
 *
 * <p>
 *   If a ProcessGroup is configured to stream data as it is available, a DataValve does not come into play.
 *   However, when two Process Groups are directly connected and configured to transfer data in/out in batches, the DataValve is
 *   responsible for ensuring that a batch of data from the source Process Group does not mix with other batches of data.
 * </p>
 *
 * <p>
 *   In order to accomplish this, an Output Port whose Process Group has a FlowFile Outbound Policy of Batch Output should not transfer
 *   data out of a Process Group unless first opening the valve via a call to {@link #tryOpenFlowOutOfGroup(ProcessGroup)} with a return
 *   value of <code>true</code>. Once the valve is open, the Input Port MUST call {@link #closeFlowOutOfGroup(ProcessGroup)} after transferring
 *   data in order to ensure that the destination Process Group is able to ingest the data.
 * </p>
 *
 * <p>
 *   Likewise, an Input Port whose Process Group has a FlowFile Concurrency of {@link FlowFileConcurrency#SINGLE_BATCH_PER_NODE} should not transfer
 *   data into the Process Group unless first opening the valve via a call to {@link #tryOpenFlowIntoGroup(ProcessGroup)} with a return value of
 *   <code>true</code>. The Input Port must then subsequently call {@link #closeFlowIntoGroup(ProcessGroup)} to enable the next batch of data to be
 *   transferred.
 * </p>
 */
public interface DataValve {

    /**
     * Attempts to open the valve such that data may flow into the given Process Group. Note that if this method returns <code>false</code>, FlowFiles
     * should NOT be transferred into the Process Group. If the method returns <code>true</code>, the Input Port may transfer data into the Process Group
     * but must then close the valve via a call to {@link #closeFlowIntoGroup(ProcessGroup)} when finished.
     *
     * @param destinationGroup the Process Group that contains the Input Port
     * @return <code>true</code> if the valve has been opened and data may flow into the Process Group, <code>false</code> if the valve could not be opened.
     */
    boolean tryOpenFlowIntoGroup(ProcessGroup destinationGroup);

    /**
     * Closes the valve such that data is no longer allowed to flow into the Process Group. This method should only be called after receiving a return value of
     * <code>true</code> from an invocation of {@link #tryOpenFlowIntoGroup(ProcessGroup)} and with the same Process Group.
     *
     * @param destinationGroup the Process Group that contains the Input Port
     */
    void closeFlowIntoGroup(ProcessGroup destinationGroup);

    /**
     * Attempts to open the valve such that data may flow out of the given Process Group. Note that if this method returns <code>false</code>, FlowFiles
     * should NOT be transferred out of the Process Group. If the method returns <code>true</code>, the Output Port may transfer data out of the Process Group
     * but must then close the valve via a call to {@link #closeFlowOutOfGroup(ProcessGroup)} when finished.
     *
     * @param sourceGroup the Process Group that contains the Output Port
     * @return <code>true</code> if the valve has been opened and data may flow out of the Process Group, <code>false</code> if the valve could not be opened.
     */
    boolean tryOpenFlowOutOfGroup(ProcessGroup sourceGroup);

    /**
     * Closes the valve such that data is no longer allowed to flow out of the Process Group. This method should only be called after receiving a return value of
     * <code>true</code> from an invocation of {@link #tryOpenFlowOutOfGroup(ProcessGroup)} and with the same Process Group.
     *
     * @param sourceGroup the Process Group that contains the Output Port
     */
    void closeFlowOutOfGroup(ProcessGroup sourceGroup);

    /**
     * @return diagnostic information about the DataValve
     */
    DataValveDiagnostics getDiagnostics();
}
