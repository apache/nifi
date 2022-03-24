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
package org.apache.nifi.annotation.notification;

/**
 * Represents a state change that occurred for the Primary Node of a NiFi cluster.
 */
public enum PrimaryNodeState {
    /**
     * The node receiving this state has been elected the Primary Node of the NiFi cluster.
     */
    ELECTED_PRIMARY_NODE,

    /**
     * The node receiving this state was the Primary Node but has now had its Primary Node
     * role revoked.
     */
    PRIMARY_NODE_REVOKED;
}
