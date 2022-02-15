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

package org.apache.nifi.c2.protocol.api;

import java.util.EnumSet;

public enum OperationState {

    /**
     * Operation has been created (assigned an ID), but has not been scheduled for deployment. Newly created operations default to this state.
     */
    NEW,

    /**
     * Operator (human or automated) has submitted the operation to be deployed to the target at some point in the future.
     */
    SUBMITTED,

    /**
     * Operation part of Bulk operation ready to be queued
     */
    READY,

    /**
     * Operation is marked to be sent to the target at the next available opportunity (i.e., in the next heartbeat response).
     */
    QUEUED,

    /**
     * Operation has been conveyed to the the target agent. It is in progress and the C2 Server is waiting for the result.
     */
    DEPLOYED,

    /**
     * The target was able to execute the operation successfully.
     */
    DONE,

    /**
     * The target was unable to execute the operation successfully.
     */
    FAILED,

    /**
     * Operation was cancelled by an operator.
     */
    CANCELLED;

    public static EnumSet<OperationState> getInProgressStates() {
        return EnumSet.of(NEW, READY, QUEUED, DEPLOYED, SUBMITTED);
    }
}
