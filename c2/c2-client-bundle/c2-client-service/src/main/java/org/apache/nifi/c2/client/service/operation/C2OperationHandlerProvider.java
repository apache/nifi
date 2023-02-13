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
package org.apache.nifi.c2.client.service.operation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2OperationHandlerProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(C2OperationHandlerProvider.class);

    private final Map<OperationType, Map<OperandType, C2OperationHandler>> handlerMap = new HashMap<>();

    public C2OperationHandlerProvider(List<C2OperationHandler> handlers) {
        for (C2OperationHandler handler : handlers) {
            handlerMap.computeIfAbsent(handler.getOperationType(), x -> new HashMap<>()).put(handler.getOperandType(), handler);
        }
    }

    public Map<OperationType, Map<OperandType, C2OperationHandler>> getHandlers() {
        Map<OperationType, Map<OperandType, C2OperationHandler>> handlers = new HashMap<>();
        handlerMap.entrySet()
            .forEach(operationEntry -> {
                Map<OperandType, C2OperationHandler> operands = new HashMap<>();
                operationEntry.getValue()
                    .entrySet()
                    .forEach(o -> operands.put(o.getKey(), o.getValue()));
                handlers.put(operationEntry.getKey(), Collections.unmodifiableMap(operands));
            });
        return Collections.unmodifiableMap(handlers);
    }

    public Optional<C2OperationHandler> getHandlerForOperation(C2Operation operation) {
        Optional<C2OperationHandler> handler = Optional.ofNullable(handlerMap.get(operation.getOperation())).map(operandMap -> operandMap.get(operation.getOperand()));
        if (!handler.isPresent()) {
            LOGGER.warn("No handler found for {} {} operation", operation.getOperation(), operation.getOperand());
        }
        return handler;
    }
}
