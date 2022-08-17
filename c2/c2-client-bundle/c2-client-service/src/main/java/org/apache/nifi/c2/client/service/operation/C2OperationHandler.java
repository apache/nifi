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

import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;

/**
 * Handler interface for the different operation types
 */
public interface C2OperationHandler {

    /**
     * Returns the supported OperationType by the handler
     *
     * @return the type of the operation
     */
    OperationType getOperationType();

    /**
     * Returns the supported OperandType by the handler
     *
     * @return the type of the operand
     */
    OperandType getOperandType();

    /**
     * Handler logic for the specific C2Operation
     *
     * @param operation the C2Operation to be handled
     * @return the result of the operation handling
     */
    C2OperationAck handle(C2Operation operation);
}
