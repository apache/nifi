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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.components.connector.components.ComponentState;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.components.connector.components.MethodArgument;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * A test processor that exposes a ConnectorMethod for performing calculations.
 * This processor is used to test JSON marshalling of complex objects across ClassLoader boundaries.
 */
public class Calculate extends AbstractProcessor {

    @ConnectorMethod(
        name = "calculate",
        description = "Performs a calculation based on the provided Calculation object",
        allowedStates = ComponentState.STOPPED,
        arguments = {
            @MethodArgument(name = "calculation", type = Calculation.class, description = "The calculation to perform", required = true)
        }
    )
    public Object calculate(final Calculation calculation) {
        final int result = switch (calculation.operation()) {
            case "ADD" -> calculation.operand1() + calculation.operand2();
            case "SUBTRACT" -> calculation.operand1() - calculation.operand2();
            case "MULTIPLY" -> calculation.operand1() * calculation.operand2();
            case "DIVIDE" -> calculation.operand1() / calculation.operand2();
            default -> throw new IllegalArgumentException("Unknown operation: " + calculation.operation());
        };

        return new CalculatedResult(calculation, result);
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
    }

    /**
     * A record representing a calculation request with two operands and an operation.
     */
    public record Calculation(int operand1, int operand2, String operation) {
    }

    /**
     * A record representing the result of a calculation, including the original calculation and the result.
     */
    public record CalculatedResult(Calculation calculation, int result) {
    }

}

