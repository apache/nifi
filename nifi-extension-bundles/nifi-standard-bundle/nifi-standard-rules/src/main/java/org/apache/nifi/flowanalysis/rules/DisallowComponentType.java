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
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedExtensionComponent;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Tags({"component", "processor", "controller service", "type"})
@CapabilityDescription("Produces rule violations for each component (i.e. processors or controller services) of a given type.")
public class DisallowComponentType extends AbstractFlowAnalysisRule {
    public static final PropertyDescriptor COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("component-type")
            .displayName("Component Type")
            .description("Components of the given type will produce a rule violation (i.e. they shouldn't exist)." +
                    " Either the simple or the fully qualified name of the type should be provided.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            COMPONENT_TYPE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
        Collection<ComponentAnalysisResult> results = new HashSet<>();

        String componentType = context.getProperty(COMPONENT_TYPE).getValue();

        if (component instanceof VersionedExtensionComponent versionedExtensionComponent) {

            String encounteredComponentType = versionedExtensionComponent.getType();
            String encounteredSimpleComponentType = encounteredComponentType.substring(encounteredComponentType.lastIndexOf(".") + 1);

            if (encounteredComponentType.equals(componentType) || encounteredSimpleComponentType.equals(componentType)) {
                ComponentAnalysisResult result = new ComponentAnalysisResult(
                        "default",
                        "'" + componentType + "' is not allowed"
                );

                results.add(result);
            }
        }

        return results;
    }
}
