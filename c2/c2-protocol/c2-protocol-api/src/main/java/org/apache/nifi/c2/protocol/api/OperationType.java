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

package org.apache.nifi.c2.protocol.api;

import static org.apache.nifi.c2.protocol.api.OperandType.ASSET;
import static org.apache.nifi.c2.protocol.api.OperandType.CONFIGURATION;
import static org.apache.nifi.c2.protocol.api.OperandType.CONNECTION;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperandType.FLOW;
import static org.apache.nifi.c2.protocol.api.OperandType.MANIFEST;
import static org.apache.nifi.c2.protocol.api.OperandType.PROPERTIES;
import static org.apache.nifi.c2.protocol.api.OperandType.RESOURCE;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum OperationType {

    // C2 Client Status Updates -> C2 Server
    ACKNOWLEDGE,
    HEARTBEAT,

    // C2 Server -> C2 Client Commands
    CLEAR(CONNECTION),
    DESCRIBE(MANIFEST),
    UPDATE(CONFIGURATION, ASSET, PROPERTIES),
    RESTART,
    START(FLOW),
    STOP(FLOW),
    PAUSE,
    REPLICATE,
    SUBSCRIBE,
    SYNC(RESOURCE),
    TRANSFER(DEBUG);

    private final Set<OperandType> supportedOperands;

    OperationType(OperandType... supportedOperands) {
        this.supportedOperands = Arrays.stream(supportedOperands).collect(Collectors.toSet());
    }

    public boolean isSupportedOperand(OperandType operand) {
        return supportedOperands.contains(operand);
    }

}
