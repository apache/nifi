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

import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.SupportedOperation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SupportedOperationsProviderTest {

    @Test
    void testSupportedOperationsAreProvided() {
        final C2OperationHandler describeManifestOperationHandler = mock(C2OperationHandler.class);
        final C2OperationHandler describeConfigurationOperationHandler = mock(C2OperationHandler.class);
        final Map<String, Object> describeManifestProperties = Collections.singletonMap("availableProperties", Arrays.asList("property1", "property2"));
        final Map<String, Object> describeConfigurationProperties = Collections.emptyMap();
        when(describeManifestOperationHandler.getProperties()).thenReturn(describeManifestProperties);
        when(describeManifestOperationHandler.getOperandType()).thenReturn(OperandType.MANIFEST);
        when(describeConfigurationOperationHandler.getProperties()).thenReturn(describeConfigurationProperties);
        when(describeConfigurationOperationHandler.getOperandType()).thenReturn(OperandType.CONFIGURATION);

        final Map<OperationType, Map<OperandType, C2OperationHandler>> operationHandlers = new HashMap<>();
        operationHandlers.put(OperationType.PAUSE, Collections.emptyMap());
        final Map<OperandType, C2OperationHandler> operandHandlers = new HashMap<>();
        operandHandlers.put(OperandType.MANIFEST, describeManifestOperationHandler);
        operandHandlers.put(OperandType.CONFIGURATION, describeConfigurationOperationHandler);
        operationHandlers.put(OperationType.DESCRIBE, operandHandlers);

        final SupportedOperationsProvider supportedOperationsProvider = new SupportedOperationsProvider(operationHandlers);

        final SupportedOperation pauseOperation = new SupportedOperation();
        pauseOperation.setType(OperationType.PAUSE);
        pauseOperation.setProperties(Collections.emptyMap());
        final SupportedOperation describeOperation = new SupportedOperation();
        describeOperation.setType(OperationType.DESCRIBE);
        final Map<OperandType, Map<String, Object>> operands = new HashMap<>();
        operands.put(OperandType.MANIFEST, describeManifestProperties);
        operands.put(OperandType.CONFIGURATION, describeConfigurationProperties);
        describeOperation.setProperties(operands);

        final Set<SupportedOperation> expected = new HashSet<>(Arrays.asList(pauseOperation, describeOperation));
        assertEquals(expected, supportedOperationsProvider.getSupportedOperations());
    }
}
