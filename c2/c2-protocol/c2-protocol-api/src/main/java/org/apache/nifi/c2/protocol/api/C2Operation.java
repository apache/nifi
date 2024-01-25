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

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;

public class C2Operation implements Serializable {
    private static final long serialVersionUID = 1L;

    private String identifier;
    private OperationType operation;
    private OperandType operand;
    private Map<String, String> args;
    private Set<String> dependencies;

    @Schema(description = "A unique identifier for the operation", accessMode = Schema.AccessMode.READ_ONLY)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Schema(description = "The type of operation")
    public OperationType getOperation() {
        return operation;
    }

    public void setOperation(OperationType operation) {
        if (operand != null && !operation.isSupportedOperand(operand)) {
            throw new IllegalArgumentException(format("%s is not a valid operand for %s", operand, operation));
        }

        this.operation = operation;
    }

    @Schema(description = "The primary operand of the operation")
    public OperandType getOperand() {
        return operand;
    }

    public void setOperand(OperandType operand) {
        if (operation != null && !operation.isSupportedOperand(operand)) {
            throw new IllegalArgumentException(format("%s is not a valid operand for %s", operand, operation));
        }

        this.operand = operand;
    }

    @Schema(description = "This is an optional field and only provided when an operation has arguments " +
            "in additional to the primary operand or optional parameters. Arguments are " +
            "arbitrary key-value pairs whose interpretation is subject to the context" +
            "of the operation and operand. For example, given:" +
            "operation=clear, operand=connection;" +
            "the args might contain the name of the connection to clear." +
            "The syntax and semantics of these arguments is defined per operation in" +
            "the C2 protocol and possibly extended by an agent's implementation of the" +
            "C2 protocol.")
    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    @Schema(description = "Optional set of operation ids that this operation depends on. " +
        "Executing this operation is conditional on the success of all dependency operations.")
    public Set<String> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<String> dependencies) {
        this.dependencies = dependencies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        C2Operation operation1 = (C2Operation) o;
        return Objects.equals(identifier, operation1.identifier) && operation == operation1.operation && operand == operation1.operand && Objects.equals(args,
            operation1.args) && Objects.equals(dependencies, operation1.dependencies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, operation, operand, args, dependencies);
    }

    @Override
    public String toString() {
        return "C2Operation{" +
            "identifier='" + identifier + '\'' +
            ", operation=" + operation +
            ", operand=" + operand +
            ", args=" + args +
            ", dependencies=" + dependencies +
            '}';
    }
}