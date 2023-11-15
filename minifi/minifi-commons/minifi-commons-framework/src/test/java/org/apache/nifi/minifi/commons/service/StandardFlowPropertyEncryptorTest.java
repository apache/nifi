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

package org.apache.nifi.minifi.commons.service;

import static java.util.Map.entry;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StandardFlowPropertyEncryptorTest {

    private static final String PROCESSOR_TYPE_1 = "processor_type_1";
    private static final String PROCESSOR_TYPE_2 = "processor_type_2";
    private static final String PROCESSOR_TYPE_3 = "processor_type_3";
    private static final String CONTROLLER_SERVICE_TYPE_1 = "controller_service_type_1";
    private static final String CONTROLLER_SERVICE_TYPE_2 = "controller_service_type_2";
    private static final String CONTROLLER_SERVICE_TYPE_3 = "controller_service_type_3";

    private static final String SENSITIVE_PROPERTY_NAME_PREFIX = "sensitive";

    private static final String NON_SENSITIVE_1 = "non-sensitive-1";
    private static final String SENSITIVE_1 = SENSITIVE_PROPERTY_NAME_PREFIX + "-1";
    private static final String NON_SENSITIVE_2 = "non-sensitive-2";
    private static final String SENSITIVE_3 = SENSITIVE_PROPERTY_NAME_PREFIX + "-3";

    private static final Map<String, String> PARAMETERS1 = Map.of(
        NON_SENSITIVE_1, NON_SENSITIVE_1,
        SENSITIVE_1, SENSITIVE_1
    );
    private static final Map<String, String> PARAMETERS2 = Map.of(
        NON_SENSITIVE_2, NON_SENSITIVE_2
    );

    private static final Map<String, String> PARAMETERS3 = Map.of(
        SENSITIVE_3, SENSITIVE_3
    );
    private static final Map<String, VersionedPropertyDescriptor> DESCRIPTORS1 = Map.of(
        NON_SENSITIVE_1, versionedPropertyDescriptor(NON_SENSITIVE_1, false),
        SENSITIVE_1, versionedPropertyDescriptor(SENSITIVE_1, true)
    );
    private static final Map<String, VersionedPropertyDescriptor> DESCRIPTORS2 = Map.of(
        NON_SENSITIVE_2, versionedPropertyDescriptor(NON_SENSITIVE_2, false)
    );
    private static final Map<String, VersionedPropertyDescriptor> DESCRIPTORS3 = Map.of(
        SENSITIVE_3, versionedPropertyDescriptor(SENSITIVE_3, true)
    );


    @Mock
    private PropertyEncryptor mockPropertyEncryptor;
    @Mock
    private RuntimeManifest mockRunTimeManifest;

    private FlowPropertyEncryptor testEncryptor;

    private static VersionedPropertyDescriptor versionedPropertyDescriptor(String name, boolean isSensitive) {
        VersionedPropertyDescriptor versionedPropertyDescriptor = new VersionedPropertyDescriptor();
        versionedPropertyDescriptor.setName(name);
        versionedPropertyDescriptor.setSensitive(isSensitive);
        return versionedPropertyDescriptor;
    }

    @BeforeEach
    public void setup() {
        when(mockPropertyEncryptor.encrypt(anyString())).thenReturn(randomAlphabetic(5));
        testEncryptor = new StandardFlowPropertyEncryptor(mockPropertyEncryptor, mockRunTimeManifest);
    }

    @Test
    public void shouldEncryptParameterContextsSensitiveVariables() {
        VersionedDataflow testFlow = flowWithParameterContexts();

        VersionedDataflow encryptedFlow = testEncryptor.encryptSensitiveProperties(testFlow);

        encryptedFlow.getParameterContexts().stream()
            .flatMap(context -> context.getParameters().stream())
            .forEach(parameter -> {
                if (parameter.isSensitive()) {
                    assertTrue(parameter.getValue().startsWith(FlowSerializer.ENC_PREFIX));
                } else {
                    assertFalse(parameter.getValue().startsWith(FlowSerializer.ENC_PREFIX));
                }
            });
    }

    @Test
    public void shouldEncryptPropertiesUsingDescriptorsFromFlow() {
        VersionedDataflow testFlow = flowWithPropertyDescriptors();

        VersionedDataflow encryptedFlow = testEncryptor.encryptSensitiveProperties(testFlow);

        verify(mockRunTimeManifest, never()).getBundles();
        assertSensitiveFlowComponentPropertiesAreEncoded(encryptedFlow);
    }

    @Test
    public void shouldEncryptPropertiesUsingDescriptorsFromRuntimeManifest() {
        VersionedDataflow testFlow = flowWithoutPropertyDescriptors();
        when(mockRunTimeManifest.getBundles()).thenReturn(runTimeManifestBundles());

        VersionedDataflow encryptedFlow = testEncryptor.encryptSensitiveProperties(testFlow);

        assertSensitiveFlowComponentPropertiesAreEncoded(encryptedFlow);
    }

    private VersionedDataflow flowWithParameterContexts() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        versionedDataflow.setRootGroup(new VersionedProcessGroup());
        versionedDataflow.setParameterContexts(
            List.of(
                parameterContext(
                    parameter(NON_SENSITIVE_1, NON_SENSITIVE_1, false),
                    parameter(SENSITIVE_1, SENSITIVE_1, true)
                ),
                parameterContext(
                    parameter(NON_SENSITIVE_2, NON_SENSITIVE_2, false)
                ),
                parameterContext(
                    parameter(SENSITIVE_3, SENSITIVE_3, true)
                )
            )
        );
        return versionedDataflow;
    }

    private VersionedParameterContext parameterContext(VersionedParameter... parameters) {
        VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setParameters(Set.of(parameters));
        return versionedParameterContext;
    }

    private VersionedParameter parameter(String name, String value, boolean sensitive) {
        VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setName(name);
        versionedParameter.setValue(value);
        versionedParameter.setSensitive(sensitive);
        return versionedParameter;
    }

    private VersionedDataflow flowWithPropertyDescriptors() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.setProcessors(
            Set.of(
                versionedProcessor(PROCESSOR_TYPE_1, PARAMETERS1, DESCRIPTORS1),
                versionedProcessor(PROCESSOR_TYPE_2, PARAMETERS2, DESCRIPTORS2),
                versionedProcessor(PROCESSOR_TYPE_3, PARAMETERS3, DESCRIPTORS3)
            ));
        versionedProcessGroup.setControllerServices(
            Set.of(
                versionedControllerService(CONTROLLER_SERVICE_TYPE_1, PARAMETERS1, DESCRIPTORS1),
                versionedControllerService(CONTROLLER_SERVICE_TYPE_2, PARAMETERS2, DESCRIPTORS2),
                versionedControllerService(CONTROLLER_SERVICE_TYPE_3, PARAMETERS3, DESCRIPTORS3)
            )
        );
        versionedDataflow.setRootGroup(versionedProcessGroup);
        return versionedDataflow;
    }

    private VersionedDataflow flowWithoutPropertyDescriptors() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.setProcessors(
            Set.of(
                versionedProcessor(PROCESSOR_TYPE_1, PARAMETERS1, Map.of()),
                versionedProcessor(PROCESSOR_TYPE_2, PARAMETERS2, Map.of()),
                versionedProcessor(PROCESSOR_TYPE_3, PARAMETERS3, Map.of())
            ));
        versionedProcessGroup.setControllerServices(
            Set.of(
                versionedControllerService(CONTROLLER_SERVICE_TYPE_1, PARAMETERS1, Map.of()),
                versionedControllerService(CONTROLLER_SERVICE_TYPE_2, PARAMETERS2, Map.of()),
                versionedControllerService(CONTROLLER_SERVICE_TYPE_3, PARAMETERS3, Map.of())
            )
        );
        versionedDataflow.setRootGroup(versionedProcessGroup);
        return versionedDataflow;
    }

    private VersionedProcessor versionedProcessor(String processorType, Map<String, String> properties, Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setIdentifier(randomUUID().toString());
        versionedProcessor.setType(processorType);
        versionedProcessor.setProperties(properties);
        versionedProcessor.setPropertyDescriptors(propertyDescriptors);
        return versionedProcessor;
    }

    private VersionedControllerService versionedControllerService(String controllerServiceType, Map<String, String> properties,
                                                                  Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        VersionedControllerService versionedControllerService = new VersionedControllerService();
        versionedControllerService.setIdentifier(randomUUID().toString());
        versionedControllerService.setType(controllerServiceType);
        versionedControllerService.setProperties(properties);
        versionedControllerService.setPropertyDescriptors(propertyDescriptors);
        return versionedControllerService;
    }

    private List<Bundle> runTimeManifestBundles() {
        return List.of(
            bundle(
                List.of(processorDefinition(PROCESSOR_TYPE_1, DESCRIPTORS1), processorDefinition(PROCESSOR_TYPE_2, DESCRIPTORS2)),
                List.of(controllerServiceDefinition(CONTROLLER_SERVICE_TYPE_1, DESCRIPTORS1))
            ),
            bundle(
                List.of(processorDefinition(PROCESSOR_TYPE_3, DESCRIPTORS3)),
                List.of(controllerServiceDefinition(CONTROLLER_SERVICE_TYPE_2, DESCRIPTORS2), controllerServiceDefinition(CONTROLLER_SERVICE_TYPE_3, DESCRIPTORS3))
            )
        );
    }

    private Bundle bundle(List<ProcessorDefinition> processorDefinition, List<ControllerServiceDefinition> controllerServiceDefinition) {
        Bundle bundle = new Bundle();
        ComponentManifest componentManifest = new ComponentManifest();
        componentManifest.setProcessors(processorDefinition);
        componentManifest.setControllerServices(controllerServiceDefinition);
        bundle.setComponentManifest(componentManifest);
        return bundle;
    }

    private ProcessorDefinition processorDefinition(String processorType, Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        ProcessorDefinition processorDefinition = new ProcessorDefinition();
        processorDefinition.setType(processorType);
        processorDefinition.setPropertyDescriptors(
            convertVersionedPropertyDescriptorMapToPropertyDescriptorMap(propertyDescriptors)
        );
        return processorDefinition;
    }

    private ControllerServiceDefinition controllerServiceDefinition(String controllerServiceType, Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        ControllerServiceDefinition controllerServiceDefinition = new ControllerServiceDefinition();
        controllerServiceDefinition.setType(controllerServiceType);
        controllerServiceDefinition.setPropertyDescriptors(
            convertVersionedPropertyDescriptorMapToPropertyDescriptorMap(propertyDescriptors)
        );
        return controllerServiceDefinition;
    }

    private LinkedHashMap<String, PropertyDescriptor> convertVersionedPropertyDescriptorMapToPropertyDescriptorMap(Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        return propertyDescriptors.values()
            .stream()
            .map(propertyDescriptor -> entry(propertyDescriptor.getName(), convertPropertyDescriptor(propertyDescriptor)))
            .collect(toMap(Entry::getKey, Entry::getValue, (l, r) -> l, LinkedHashMap::new));
    }

    private PropertyDescriptor convertPropertyDescriptor(VersionedPropertyDescriptor versionedPropertyDescriptor) {
        PropertyDescriptor propertyDescriptor = new PropertyDescriptor();
        propertyDescriptor.setName(versionedPropertyDescriptor.getName());
        propertyDescriptor.setSensitive(versionedPropertyDescriptor.isSensitive());
        return propertyDescriptor;
    }

    private void assertSensitiveFlowComponentPropertiesAreEncoded(VersionedDataflow encryptedFlow) {
        Stream.of(
                encryptedFlow.getRootGroup().getProcessors(),
                encryptedFlow.getRootGroup().getControllerServices()
            )
            .flatMap(Set::stream)
            .map(VersionedConfigurableExtension::getProperties)
            .flatMap(properties -> properties.entrySet().stream())
            .forEach(propertyEntry -> {
                    if (propertyEntry.getKey().startsWith(SENSITIVE_PROPERTY_NAME_PREFIX)) {
                        assertTrue(propertyEntry.getValue().startsWith(FlowSerializer.ENC_PREFIX));
                    } else {
                        assertFalse(propertyEntry.getValue().startsWith(FlowSerializer.ENC_PREFIX));
                    }
                }
            );
    }
}
