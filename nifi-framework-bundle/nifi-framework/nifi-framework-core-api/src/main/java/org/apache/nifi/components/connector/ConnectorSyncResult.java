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

package org.apache.nifi.components.connector;

/**
 * Result of a connector synchronization attempt during flow inheritance.
 */
public enum ConnectorSyncResult {

    /**
     * Configuration was applied and the connector's run state was updated
     * to match the proposed ScheduledState.
     */
    SYNCED,

    /**
     * Configuration was already up to date; the connector's run state was
     * updated if it differed from the proposed ScheduledState.
     */
    SYNCED_NO_CHANGES,

    /**
     * The connector was in a state that prevents synchronization (e.g.,
     * DRAINING, PREPARING_FOR_UPDATE, UPDATING) or a wait for a transient
     * state timed out. The connector has been marked invalid.
     */
    REJECTED,

    /**
     * Synchronization was attempted but failed due to an unrecoverable
     * error. The connector has been marked invalid.
     */
    FAILED,

    /**
     * The connector was removed from this node because the external
     * provider indicated it should not exist (e.g., connector is being
     * deleted or has been deleted in the external system).
     */
    REMOVED
}
