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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.SupportedOperation;

public class SupportedOperationsProvider {
    private final Map<OperationType, Map<OperandType, C2OperationHandler>> operationHandlers;

    public SupportedOperationsProvider(Map<OperationType, Map<OperandType, C2OperationHandler>> handlers) {
        operationHandlers = handlers;
    }

    public Set<SupportedOperation> getSupportedOperations() {
        return operationHandlers.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(operationEntry -> getSupportedOperation(operationEntry.getKey(), operationEntry.getValue()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private SupportedOperation getSupportedOperation(OperationType operationType, Map<OperandType, C2OperationHandler> operands) {
        SupportedOperation supportedOperation = new SupportedOperation();
        supportedOperation.setType(operationType);

        Map<OperandType, Map<String, Object>> properties = operands.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Entry::getValue)
            .collect(
                Collectors.toMap(
                    C2OperationHandler::getOperandType,
                    C2OperationHandler::getProperties,
                    (existing, replacement) -> existing,
                    LinkedHashMap::new
                ));

        supportedOperation.setProperties(properties);

        return supportedOperation;
    }

}
