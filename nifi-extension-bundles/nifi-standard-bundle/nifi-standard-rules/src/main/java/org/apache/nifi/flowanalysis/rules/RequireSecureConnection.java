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
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedExtensionComponent;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


@Tags({"component", "processor", "controller service", "type"})
@CapabilityDescription("Produces rule violations for each component (i.e. processors or controller services) having a property "
        + "identifying an SSLContextService that is not set.")
@UseCase(
        description = "Ensure that no ports can be opened for insecure (plaintext, e.g.) communications.",
        configuration = """
                To avoid the violation, ensure that the "SSL Context Service" property is set for the specified component(s).
                """
)
public class RequireSecureConnection extends AbstractFlowAnalysisRule {
    public static final PropertyDescriptor COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("component-type")
            .displayName("Component Type(s)")
            .description("A comma-separated list of component types. Components of the given type that have a property identifying an SSL Context Service will produce a rule violation "
                    + "if the service is not set. If no components are specified (i.e. this property is blank), the rule will apply to all components.")
            .required(false)
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
        Set<String> componentTypes = new LinkedHashSet<>();
        if (context.getProperty(COMPONENT_TYPE).isSet()) {
            String componentTypesList = context.getProperty(COMPONENT_TYPE).getValue();
            if (componentTypesList != null) {
                Arrays.stream(componentTypesList.split(","))
                        .filter(StringUtils::isNotBlank)
                        .map(String::trim)
                        .forEach(componentTypes::add);
            }
        }

        String componentType = context.getProperty(COMPONENT_TYPE).getValue();

        if (component instanceof VersionedExtensionComponent versionedExtensionComponent) {

            String encounteredComponentType = versionedExtensionComponent.getType();
            String encounteredSimpleComponentType = encounteredComponentType.substring(encounteredComponentType.lastIndexOf(".") + 1);

            if (componentTypes.isEmpty() || componentTypes.contains(encounteredComponentType) || componentType.contains(encounteredSimpleComponentType)) {
                Set<PropertyDescriptor> propertyDescriptors = context.getProperties().keySet();
                // Loop over the properties for this component looking for an SSLContextService
                for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                    Class<? extends ControllerService> definition = propertyDescriptor.getControllerServiceDefinition();
                    if (definition == null) {
                        continue;
                    }
                    try {
                        // Is this an SSLContextService?
                        definition.asSubclass(SSLContextProvider.class);

                        // If it is and the property value is not set, report a violation
                        if (!context.getProperty(propertyDescriptor).isSet()) {
                            ComponentAnalysisResult result = new ComponentAnalysisResult(
                                    component.getInstanceIdentifier(),
                                    "'" + componentType + "' is not allowed"
                            );

                            results.add(result);
                        }
                    } catch (ClassCastException cce) {
                        // Do nothing, this is not the property we're looking for
                        continue;
                    }
                }
            }
        }

        return results;
    }
}
