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

public enum LoadBalanceStrategy {
    /**
     * Do not load balance FlowFiles between nodes in the cluster.
     */
    DO_NOT_LOAD_BALANCE,

    /**
     * Determine which node to send a given FlowFile to based on the value of a user-specified FlowFile Attribute.
     * All FlowFiles that have the same value for said Attribute will be sent to the same node in the cluster.
     */
    PARTITION_BY_ATTRIBUTE,

    /**
     * FlowFiles will be distributed to nodes in the cluster in a Round-Robin fashion.
     */
    ROUND_ROBIN,

    /**
     * All FlowFiles will be sent to the same node. Which node they are sent to is not defined.
     */
    SINGLE_NODE;
}
