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
package org.apache.nifi.cluster.protocol.message;

public abstract class ProtocolMessage {

    public static enum MessageType {
        CONNECTION_REQUEST,
        CONNECTION_RESPONSE,
        OFFLOAD_REQUEST,
        DISCONNECTION_REQUEST,
        EXCEPTION,
        FLOW_REQUEST,
        FLOW_RESPONSE,
        PING,
        RECONNECTION_REQUEST,
        RECONNECTION_RESPONSE,
        SERVICE_BROADCAST,
        HEARTBEAT,
        HEARTBEAT_RESPONSE,
        NODE_CONNECTION_STATUS_REQUEST,
        NODE_CONNECTION_STATUS_RESPONSE,
        NODE_STATUS_CHANGE,
        CLUSTER_WORKLOAD_REQUEST,
        CLUSTER_WORKLOAD_RESPONSE
    }

    public abstract MessageType getType();

}
