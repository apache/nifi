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

package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.StepConfigurationContext;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A test connector that invokes a ConnectorMethod on a CalculateProcessor using its own POJO types.
 * This connector is used to test JSON marshalling of complex objects across ClassLoader boundaries.
 */
public class CalculateConnector extends AbstractConnector {

    /**
     * A POJO representing a calculation request. This is intentionally a different class than
     * the Processor's record type to test cross-ClassLoader JSON marshalling.
     */
    public static class Calculation {
        private int operand1;
        private int operand2;
        private String operation;

        public Calculation() {
        }

        public Calculation(final int operand1, final int operand2, final String operation) {
            this.operand1 = operand1;
            this.operand2 = operand2;
            this.operation = operation;
        }

        public int getOperand1() {
            return operand1;
        }

        public void setOperand1(final int operand1) {
            this.operand1 = operand1;
        }

        public int getOperand2() {
            return operand2;
        }

        public void setOperand2(final int operand2) {
            this.operand2 = operand2;
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(final String operation) {
            this.operation = operation;
        }
    }

    /**
     * A POJO representing the result of a calculation. This is intentionally a different class than
     * the Processor's record type to test cross-ClassLoader JSON marshalling.
     */
    public static class CalculatedResult {
        private Calculation calculation;
        private int result;

        public CalculatedResult() {
        }

        public Calculation getCalculation() {
            return calculation;
        }

        public void setCalculation(final Calculation calculation) {
            this.calculation = calculation;
        }

        public int getResult() {
            return result;
        }

        public void setResult(final int result) {
            this.result = result;
        }
    }

    private static final ConnectorPropertyDescriptor OPERAND_1 = new ConnectorPropertyDescriptor.Builder()
        .name("Operand 1")
        .description("The first operand for the calculation")
        .type(PropertyType.STRING)
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor OPERAND_2 = new ConnectorPropertyDescriptor.Builder()
        .name("Operand 2")
        .description("The second operand for the calculation")
        .type(PropertyType.STRING)
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor OPERATION = new ConnectorPropertyDescriptor.Builder()
        .name("Operation")
        .description("The operation to perform (ADD, SUBTRACT, MULTIPLY, DIVIDE)")
        .type(PropertyType.STRING)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor OUTPUT_FILE = new ConnectorPropertyDescriptor.Builder()
        .name("Output File")
        .description("The file to write the calculation result to")
        .type(PropertyType.STRING)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup CALCULATION_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Calculation Configuration")
        .description("Configuration properties for the calculation")
        .properties(List.of(OPERAND_1, OPERAND_2, OPERATION, OUTPUT_FILE))
        .build();

    private static final ConfigurationStep CALCULATION_STEP = new ConfigurationStep.Builder()
        .name("Calculation")
        .description("Configure the calculation parameters")
        .propertyGroups(List.of(CALCULATION_GROUP))
        .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Calculate Flow");
        group.setIdentifier("calculate-flow-id");

        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-system-test-extensions-nar");
        bundle.setVersion("2.7.0-SNAPSHOT");

        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("calculate-processor-id");
        processor.setName("Calculate Processor");
        processor.setType("org.apache.nifi.processors.tests.system.Calculate");
        processor.setBundle(bundle);
        processor.setProperties(Map.of());
        processor.setPropertyDescriptors(Map.of());
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setBulletinLevel("WARN");
        processor.setPosition(new Position(0D, 0D));
        processor.setPenaltyDuration("30 sec");
        processor.setAutoTerminatedRelationships(Set.of());
        processor.setExecutionNode("ALL");
        processor.setGroupIdentifier(group.getIdentifier());
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setRunDurationMillis(0L);
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setYieldDuration("1 sec");
        processor.setSchedulingPeriod("0 sec");
        processor.setStyle(Map.of());
        group.setProcessors(Set.of(processor));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        flow.setParameterContexts(Map.of());
        return flow;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CALCULATION_STEP);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final StepConfigurationContext stepContext = workingContext.getConfigurationContext().scopedToStep(CALCULATION_STEP);
        final int operand1 = stepContext.getProperty(OPERAND_1).asInteger();
        final int operand2 = stepContext.getProperty(OPERAND_2).asInteger();
        final String operation = stepContext.getProperty(OPERATION).getValue();
        final String outputFile = stepContext.getProperty(OUTPUT_FILE).getValue();

        final ProcessorFacade processorFacade = workingContext.getRootGroup().getProcessors().stream()
            .filter(p -> p.getDefinition().getType().endsWith("Calculate"))
            .findFirst()
            .orElseThrow(() -> new FlowUpdateException("CalculateProcessor not found in flow"));

        final Calculation calculation = new Calculation(operand1, operand2, operation);
        final CalculatedResult result;
        try {
            result = processorFacade.invokeConnectorMethod("calculate", Map.of("calculation", calculation), CalculatedResult.class);
        } catch (final InvocationFailedException e) {
            throw new FlowUpdateException("Failed to invoke calculate method", e);
        }

        final File file = new File(outputFile);
        try (final FileWriter writer = new FileWriter(file)) {
            writer.write(String.valueOf(result.getCalculation().getOperand1()));
            writer.write("\n");
            writer.write(String.valueOf(result.getCalculation().getOperand2()));
            writer.write("\n");
            writer.write(result.getCalculation().getOperation());
            writer.write("\n");
            writer.write(String.valueOf(result.getResult()));
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to write result to file", e);
        }

        getLogger().info("Calculation result: {} {} {} = {}", operand1, operation, operand2, result.getResult());
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }
}

