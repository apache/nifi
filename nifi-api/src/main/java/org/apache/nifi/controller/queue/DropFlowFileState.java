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
 * Represents the state that a Drop FlowFile request is in
 */
public enum DropFlowFileState {

    WAITING_FOR_LOCK("Waiting for Destination Component to complete its action"),
    DROPPING_ACTIVE_FLOWFILES("Dropping FlowFiles from queue"),
    COMPLETE("Completed Successfully"),
    FAILURE("Failed"),
    CANCELED("Cancelled by User");

    private final String description;

    private DropFlowFileState(final String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }
}
