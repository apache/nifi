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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import org.apache.nifi.c2.protocol.api.C2Operation;

public class OperationQueue implements Serializable {
    private static final long serialVersionUID = 1L;

    private C2Operation currentOperation;
    private List<C2Operation> remainingOperations;

    public static OperationQueue create(C2Operation currentOperation, Queue<C2Operation> remainingOperations) {
        return new OperationQueue(
            currentOperation,
            ofNullable(remainingOperations)
                .map(queue -> queue.stream().toList())
                .orElseGet(List::of)
        );

    }

    public OperationQueue() {
    }

    public OperationQueue(C2Operation currentOperation, List<C2Operation> remainingOperations) {
        this.currentOperation = currentOperation;
        this.remainingOperations = remainingOperations;

    }

    public C2Operation getCurrentOperation() {
        return currentOperation;
    }

    public List<C2Operation> getRemainingOperations() {
        return remainingOperations;
    }

    public void setCurrentOperation(C2Operation currentOperation) {
        this.currentOperation = currentOperation;
    }

    public void setRemainingOperations(List<C2Operation> remainingOperations) {
        this.remainingOperations = remainingOperations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OperationQueue that = (OperationQueue) o;
        return Objects.equals(currentOperation, that.currentOperation) && Objects.equals(remainingOperations, that.remainingOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentOperation, remainingOperations);
    }

    @Override
    public String toString() {
        return "OperationQueue{" +
            "currentOperation=" + currentOperation +
            ", remainingOperations=" + remainingOperations +
            '}';
    }
}
