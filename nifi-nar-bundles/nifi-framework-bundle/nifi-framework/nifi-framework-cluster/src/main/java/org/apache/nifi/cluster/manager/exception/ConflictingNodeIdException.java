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

package org.apache.nifi.cluster.manager.exception;

public class ConflictingNodeIdException extends Exception {
    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final String conflictingNodeAddress;
    private final int conflictingNodePort;

    public ConflictingNodeIdException(final String nodeId, final String conflictingNodeAddress, final int conflictingNodePort) {
        super("Node Identifier " + nodeId + " conflicts with existing node " + conflictingNodeAddress + ":" + conflictingNodePort);

        this.nodeId = nodeId;
        this.conflictingNodeAddress = conflictingNodeAddress;
        this.conflictingNodePort = conflictingNodePort;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getConflictingNodeAddress() {
        return conflictingNodeAddress;
    }

    public int getConflictingNodePort() {
        return conflictingNodePort;
    }
}
