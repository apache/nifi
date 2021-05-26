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

package org.apache.nifi.registry.flow.diff;

public enum DifferenceType {
    /**
     * The component does not exist in Flow A but exists in Flow B
     */
    COMPONENT_ADDED("Component Added"),

    /**
     * The component exists in Flow A but does not exist in Flow B
     */
    COMPONENT_REMOVED("Component Removed"),

    /**
     * The component has a different name in each of the flows
     */
    NAME_CHANGED("Component Name Changed"),

    /**
     * The component has a different type in each of the flows
     */
    TYPE_CHANGED("Component Type Changed"),

    /**
     * The component has a different bundle in each of the flows
     */
    BUNDLE_CHANGED("Component Bundle Changed"),

    /**
     * The component has a different penalty duration in each of the flows
     */
    PENALTY_DURATION_CHANGED("Penalty Duration Changed"),

    /**
     * The component has a different yield duration in each of the flows
     */
    YIELD_DURATION_CHANGED("Yield Duration Changed"),

    /**
     * The component has a different bulletin level in each of the flows
     */
    BULLETIN_LEVEL_CHANGED("Bulletin Level Changed"),

    /**
     * The component has a different set of Auto-Terminated Relationships in each of the flows
     */
    AUTO_TERMINATED_RELATIONSHIPS_CHANGED("Auto-Terminated Relationships Changed"),

    /**
     * The component has a different scheduling strategy in each of the flows
     */
    SCHEDULING_STRATEGY_CHANGED("Scheduling Strategy Changed"),

    /**
     * The component has a different maximum number of concurrent tasks in each of the flows
     */
    CONCURRENT_TASKS_CHANGED("Concurrent Tasks Changed"),

    /**
     * The component has a different run schedule in each of the flows
     */
    RUN_SCHEDULE_CHANGED("Run Schedule Changed"),

    /**
     * The component has a different scheduled state (enabled/disabled) in each of the flows
     */
    SCHEDULED_STATE_CHANGED("Scheduled State Changed"),

    /**
     * The component has a different execution mode in each of the flows
     */
    EXECUTION_MODE_CHANGED("Execution Mode Changed"),

    /**
     * The component has a different run duration in each of the flows
     */
    RUN_DURATION_CHANGED("Run Duration Changed"),

    /**
     * The component has a different value in each of the flows for a specific property
     */
    PROPERTY_CHANGED("Property Value Changed"),

    /**
     * Property does not exist in Flow A but does exist in Flow B
     */
    PROPERTY_ADDED("Property Added"),

    /**
     * Property exists in Flow A but does not exist in Flow B
     */
    PROPERTY_REMOVED("Property Removed"),

    /**
     * Property is unset or set to an explicit value in Flow A but set to (exactly) a parameter reference in Flow B. Note that if Flow A
     * has a property set to "#{param1} abc" and it is changed to "#{param1} abc #{param2}" this would indicate a Difference Type of @{link #PROPERTY_CHANGED}, not
     * PROPERTY_PARAMETERIZED
     */
    PROPERTY_PARAMETERIZED("Property Parameterized"),

    /**
     * Property is set to (exactly) a parameter reference in Flow A but either unset or set to an explicit value in Flow B.
     */
    PROPERTY_PARAMETERIZATION_REMOVED("Property Parameterization Removed"),

    /**
     * The component has a different value for the Annotation Data in each of the flows
     */
    ANNOTATION_DATA_CHANGED("Annotation Data (Advanced UI Configuration) Changed"),

    /**
     * The component has a different comment in each of the flows
     */
    COMMENTS_CHANGED("Comments Changed"),

    /**
     * The position of the component on the graph is different in each of the flows
     */
    POSITION_CHANGED("Position Changed"),

    /**
     * The stylistic configuration of the component is different in each of the flows
     */
    STYLE_CHANGED("Style Changed"),

    /**
     * The Relationships included in a connection is different in each of the flows
     */
    SELECTED_RELATIONSHIPS_CHANGED("Selected Relationships Changed"),

    /**
     * The Connection has a different set of Prioritizers in each of the flows
     */
    PRIORITIZERS_CHANGED("Prioritizers Changed"),

    /**
     * The Connection has a different value for the FlowFile Expiration in each of the flows
     */
    FLOWFILE_EXPIRATION_CHANGED("FlowFile Expiration Changed"),

    /**
     * The Connection has a different value for the Object Backpressure Threshold in each of the flows
     */
    BACKPRESSURE_OBJECT_THRESHOLD_CHANGED("Backpressure Object Threshold Changed"),

    /**
     * The Connection has a different value for the Data Size Backpressure Threshold in each of the flows
     */
    BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED("Backpressure Data Size Threshold Changed"),

    /**
     * The Connection has a different value for the Load Balance Strategy in each of the flows
     */
    LOAD_BALANCE_STRATEGY_CHANGED("Load-Balance Strategy Changed"),

    /**
     * The Connection has a different value for the Partitioning Attribute in each of the flows
     */
    PARTITIONING_ATTRIBUTE_CHANGED("Partitioning Attribute Changed"),

    /**
     * The Connection has a different value for the Load Balancing Compression in each of the flows
     */
    LOAD_BALANCE_COMPRESSION_CHANGED("Load-Balance Compression Changed"),

    /**
     * The Connection has a different set of Bend Points in each of the flows
     */
    BENDPOINTS_CHANGED("Connection Bend Points Changed"),

    /**
     * The Connection has a difference Source in each of the flows
     */
    SOURCE_CHANGED("Connection Source Changed"),

    /**
     * The Connection has a difference Destination in each of the flows
     */
    DESTINATION_CHANGED("Connection Destination Changed"),

    /**
     * The value in the Label is different in each of the flows
     */
    LABEL_VALUE_CHANGED("Label Text Changed"),

    /**
     * The variable does not exist in Flow A but exists in Flow B
     */
    VARIABLE_ADDED("Variable Added to Process Group"),

    /**
     * The variable does not exist in Flow B but exists in Flow A
     */
    VARIABLE_REMOVED("Variable Removed from Process Group"),

    /**
     * The API of the Controller Service is different in each of the flows
     */
    SERVICE_API_CHANGED("Controller Service API Changed"),

    /**
     * The Remote Process Group has a different Transport Protocol in each of the flows
     */
    RPG_TRANSPORT_PROTOCOL_CHANGED("Remote Process Group Transport Protocol Changed"),

    /**
     * The Remote Process Group has a different Proxy Host in each of the flows
     */
    RPG_PROXY_HOST_CHANGED("Remote Process Group Proxy Host Changed"),

    /**
     * The Remote Process Group has a different Proxy Port in each of the flows
     */
    RPG_PROXY_PORT_CHANGED("Remote Process Group Proxy Port Changed"),

    /**
     * The Remote Process Group has a different Proxy User in each of the flows
     */
    RPG_PROXY_USER_CHANGED("Remote Process Group Proxy User Changed"),

    /**
     * The Remote Process Group has a different Network Interface chosen in each of the flows
     */
    RPG_NETWORK_INTERFACE_CHANGED("Remote Process Group Network Interface Changed"),

    /**
     * The Remote Process Group has a different Communications Timeout in each of the flows
     */
    RPG_COMMS_TIMEOUT_CHANGED("Remote Process Group Communications Timeout Changed"),

    /**
     * The Remote Input Port or Remote Output Port has a different Batch Size in each of the flows
     */
    REMOTE_PORT_BATCH_SIZE_CHANGED("Remote Process Group Port's Batch Size Changed"),

    /**
     * The Remote Input Port or Remote Output Port has a different value for the Compression flag in each of the flows
     */
    REMOTE_PORT_COMPRESSION_CHANGED("Remote Process Group Port's Compression Flag Changed"),

    /**
     * The Process Group points to a different Versioned Flow in each of the flows
     */
    VERSIONED_FLOW_COORDINATES_CHANGED("Versioned Flow Coordinates Changed"),

    /**
     * The Process Group's configured FlowFile Concurrency is different in each of the flows
     */
    FLOWFILE_CONCURRENCY_CHANGED("FlowFile Concurrency Changed"),

    /**
     * The Process Group's configured FlowFile Outbound Policy is different in each of the flows
     */
    FLOWFILE_OUTBOUND_POLICY_CHANGED("FlowFile Outbound Policy Changed");

    private final String description;

    DifferenceType(final String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
