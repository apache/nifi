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
package org.apache.nifi.analyzeflow.ruleimpl;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowDetails;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class DisallowProcessorType extends AbstractFlowAnalysisRule {
    public static final PropertyDescriptor PROCESSOR_TYPE = new PropertyDescriptor.Builder()
        .name("processor-type")
        .displayName("Processor Type")
        .description("Disallows processors of a given type.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .defaultValue(null)
        .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(PROCESSOR_TYPE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Collection<FlowAnalysisResult> analyzeFlow(
        String ruleName,
        FlowAnalysisRuleContext context,
        FlowDetails flowDetails,
        Function<String, VersionedControllerService> controllerServiceDetailsProvider
    ) {
        Collection<FlowAnalysisResult> result = new ArrayList<>();

        String processorType = context.getProperty(PROCESSOR_TYPE).getValue();

        for (VersionedProcessor processor : flowDetails.getProcessors()) {
            String encounteredProcessorType = processor.getType();
            encounteredProcessorType = encounteredProcessorType.substring(encounteredProcessorType.lastIndexOf(".") + 1);

            if (encounteredProcessorType.equals(processorType)) {
                FlowAnalysisResult flowAnalysisResult = new FlowAnalysisResult(
                    processor.getIdentifier(),
                    ruleName,
                    "'" + processorType + "' is not allowed!"
                );

                result.add(flowAnalysisResult);
            }
        }

        return result;
    }
}
