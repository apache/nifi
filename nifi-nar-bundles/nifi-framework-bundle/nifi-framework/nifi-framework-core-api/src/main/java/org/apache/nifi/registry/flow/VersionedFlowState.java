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

package org.apache.nifi.registry.flow;

public enum VersionedFlowState {

    /**
     * We are unable to communicate with the Flow Registry in order to determine the appropriate state
     */
    SYNC_FAILURE("Failed to communicate with Flow Registry"),

    /**
     * This Process Group (or a child/descendant Process Group that is not itself under Version Control)
     * is on the latest version of the Versioned Flow, but is different than the Versioned Flow that is
     * stored in the Flow Registry.
     */
    LOCALLY_MODIFIED("Local changes have been made"),

    /**
     * This Process Group has not been modified since it was last synchronized with the Flow Registry, but
     * the Flow Registry has a newer version of the flow than what is contained in this Process Group.
     */
    STALE("A newer version of this flow is available"),

    /**
     * This Process Group (or a child/descendant Process Group that is not itself under Version Control)
     * has been modified since it was last synchronized with the Flow Registry, and the Flow Registry has
     * a newer version of the flow than what is contained in this Process Group.
     */
    LOCALLY_MODIFIED_AND_STALE("Local changes have been made and a newer version of this flow is available"),

    /**
     * This Process Group and all child/descendant Process Groups are on the latest version of the flow in
     * the Flow Registry and have no local modifications.
     */
    UP_TO_DATE("Flow version is current");


    private final String description;

    private VersionedFlowState(final String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
