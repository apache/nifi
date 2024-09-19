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

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.FailureCause;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.apache.nifi.minifi.validator.ValidationException;

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
     * Returns the properties context for the given operand
     *
     * @return the property map
     */
    Map<String, Object> getProperties();

    /**
     * Determines if the given operation requires to restart the MiNiFi process
     *
     * @return true if it requires restart, false otherwise
     */
    default boolean requiresRestart() {
        return false;
    }

    /**
     * Handler logic for the specific C2Operation
     *
     * @param operation the C2Operation to be handled
     * @return the result of the operation handling
     */
    C2OperationAck handle(C2Operation operation);

    /**
     * Commonly used logic for creating an C2OperationState object
     *
     * @param operationState the state of the operation
     * @param details        additional status info to detail the state
     * @return the created state
     */
    default C2OperationState operationState(C2OperationState.OperationState operationState, String details, Exception e) {
        C2OperationState state = new C2OperationState();
        state.setState(operationState);
        state.setDetails(details);
        ofNullable(e).map(this::toFailureCause).ifPresent(state::setFailureCause);
        return state;
    }

    private FailureCause toFailureCause(Exception exception) {
        FailureCause failureCause = new FailureCause();
        failureCause.setExceptionMessage(exception.getMessage());
        List<String> causeList = new LinkedList<>();
        populateCausedChain(ofNullable(exception.getCause()), causeList);
        failureCause.setCausedByMessages(causeList);
        if (exception instanceof ValidationException validationException) {
            failureCause.setValidationResults(validationException.getValidationResults());
        }
        return failureCause;
    }

    private List<String> populateCausedChain(Optional<Throwable> cause, List<String> causeList) {
        cause.ifPresent(c -> {
            causeList.add(c.getMessage());
            populateCausedChain(cause.map(Throwable::getCause), causeList);
        });
        return causeList;
    }

    default C2OperationState operationState(C2OperationState.OperationState operationState, String details) {
        return operationState(operationState, details, null);
    }

    /**
     * Commonly used logic for creating an C2OperationAck object
     *
     * @param operationId    the identifier of the operation
     * @param operationState the state of the operation
     * @return the created operation ack object
     */
    default C2OperationAck operationAck(String operationId, C2OperationState operationState) {
        C2OperationAck operationAck = new C2OperationAck();
        operationAck.setOperationState(operationState);
        operationAck.setOperationId(operationId);
        return operationAck;
    }

    /**
     * Commonly used logic for retrieving a string value from the operation arguments' map
     *
     * @param operation the operation with arguments
     * @param argument  the name of the argument to retrieve
     * @return the optional retrieved argument value
     */
    default Optional<String> getOperationArg(C2Operation operation, String argument) {
        return ofNullable(operation.getArgs())
            .map(args -> args.get(argument))
            .map(arg -> arg instanceof String s ? s : null);
    }

    /**
     * Commonly used logic for retrieving a JSON object from the operation arguments' map and converting it to a model class
     *
     * @param operation  the operation with arguments
     * @param argument   the name of the argument to retrieve
     * @param type       the target type to serialize the object to
     * @param serializer the serializer used to converting to the target class
     * @return the optional retrieved and converted argument value
     */
    default <T> T getOperationArg(C2Operation operation, String argument, TypeReference<T> type, C2Serializer serializer) {
        return ofNullable(operation.getArgs())
            .map(args -> args.get(argument))
            .flatMap(arg -> serializer.convert(arg, type))
            .orElseThrow(() -> new IllegalArgumentException("Failed to parse argument " + argument + " of operation " + operation));
    }
}
