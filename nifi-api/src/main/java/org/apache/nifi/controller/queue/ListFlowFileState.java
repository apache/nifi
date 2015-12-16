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

/**
 * Represents the state that a List FlowFile Request is in
 */
public enum ListFlowFileState {
    WAITING_FOR_LOCK("Waiting for other queue requests to complete"),
    CALCULATING_LIST("Calculating list of FlowFiles"),
    FAILURE("Failed"),
    CANCELED("Canceled by user"),
    COMPLETE("Completed successfully");

    private final String description;

    private ListFlowFileState(final String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * @param description string form of list flow file state
     * @return the matching ListFlowFileState or <code>null</code> if the description doesn't match
     */
    public static ListFlowFileState valueOfDescription(String description) {
        ListFlowFileState desiredState = null;

        for (ListFlowFileState state : values()) {
            if (state.toString().equals(description)) {
                desiredState = state;
                break;
            }
        }

        return desiredState;
    }
}
