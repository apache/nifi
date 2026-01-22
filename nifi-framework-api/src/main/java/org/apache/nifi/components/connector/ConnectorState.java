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
 * Represents the lifecycle state of a Connector.
 */
public enum ConnectorState {
    /**
     * The connector is in the process of starting.
     */
    STARTING,

    /**
     * The connector is running and processing data.
     */
    RUNNING,

    /**
     * The connector is in the process of stopping.
     */
    STOPPING,

    /**
     * The connector is stopped and not processing data.
     */
    STOPPED,

    /**
     * The connector is draining in-flight data before stopping.
     */
    DRAINING,

    /**
     * The connector is purging all queued data.
     */
    PURGING,

    /**
     * The connector is preparing to receive a configuration update.
     */
    PREPARING_FOR_UPDATE,

    /**
     * The connector is being updated with new configuration.
     */
    UPDATING,

    /**
     * The connector update failed.
     */
    UPDATE_FAILED,

    /**
     * The connector has been successfully updated.
     */
    UPDATED
}
