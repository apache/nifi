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

package org.apache.nifi.controller.cluster;

import java.io.Closeable;
import java.io.IOException;

import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;

/**
 * <p>
 * A mechanism for sending a heartbeat to a remote resource to indicate
 * that the node is still an active participant in the cluster
 * <p>
 */
public interface Heartbeater extends Closeable {

    /**
     * Sends the given heartbeat to the remote resource
     *
     * @param heartbeat the Heartbeat to send
     * @throws IOException if unable to communicate with the remote resource
     */
    void send(HeartbeatMessage heartbeat) throws IOException;

    /**
     * @return the address that heartbeats are being sent to
     */
    String getHeartbeatAddress() throws IOException;
}
