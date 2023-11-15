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

import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.nifi.c2.protocol.component.api.DefinedType;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedPropertyDescriptor;

public class StandardFlowPropertyEncryptor implements FlowPropertyEncryptor {

    private static final String ENCRYPTED_FORMAT = "enc{%s}";

    private final PropertyEncryptor propertyEncryptor;
    private final RuntimeManifest runTimeManifest;

    public StandardFlowPropertyEncryptor(PropertyEncryptor propertyEncryptor, RuntimeManifest runTimeManifest) {
        this.propertyEncryptor = propertyEncryptor;
        this.runTimeManifest = runTimeManifest;
    }

    @Override
    public VersionedDataflow encryptSensitiveProperties(VersionedDataflow flow) {
        encryptParameterContextsProperties(flow);

        Map<String, Set<String>> sensitivePropertiesByComponentType = Optional.of(flowProvidedSensitiveProperties(flow))
            .filter(not(Map::isEmpty))
            .orElseGet(this::runtimeManifestSensitiveProperties);

        encryptFlowComponentsProperties(flow, sensitivePropertiesByComponentType);

        return flow;
    }

    private void encryptParameterContextsProperties(VersionedDataflow flow) {
        ofNullable(flow.getParameterContexts())
            .orElse(List.of())
            .forEach(parameterContext -> ofNullable(parameterContext.getParameters()).orElse(Set.of())
                .stream()
                .filter(VersionedParameter::isSensitive)
                .filter(not(parameter -> ofNullable(parameter.getValue()).orElse(EMPTY).startsWith(FlowSerializer.ENC_PREFIX)))
                .forEach(parameter -> parameter.setValue(encrypt(parameter.getValue()))));
    }

    private Map<String, Set<String>> flowProvidedSensitiveProperties(VersionedDataflow flow) {
        return fetchFlowComponents(flow)
            .map(extension -> Map.entry(
                extension.getType(),
                ofNullable(extension.getPropertyDescriptors()).orElse(Map.of())
                    .values()
                    .stream()
                    .filter(VersionedPropertyDescriptor::isSensitive)
                    .map(VersionedPropertyDescriptor::getName)
                    .collect(toSet())
            ))
            .filter(not(entry -> entry.getValue().isEmpty()))
            .collect(toMap(Entry::getKey, Entry::getValue, this::mergeSets));
    }

    private Map<String, Set<String>> runtimeManifestSensitiveProperties() {
        return ofNullable(runTimeManifest.getBundles()).orElse(List.of())
            .stream()
            .flatMap(bundle -> Stream.of(
                ofNullable(bundle.getComponentManifest().getProcessors()).orElse(List.of()),
                ofNullable(bundle.getComponentManifest().getControllerServices()).orElse(List.of())
            ))
            .flatMap(List::stream)
            .collect(toMap(
                DefinedType::getType,
                type -> ofNullable(type.getPropertyDescriptors()).orElse(Map.of())
                    .values()
                    .stream()
                    .filter(PropertyDescriptor::getSensitive)
                    .map(PropertyDescriptor::getName)
                    .collect(toSet()),
                this::mergeSets
            ));
    }

    private void encryptFlowComponentsProperties(VersionedDataflow flow, Map<String, Set<String>> sensitivePropertiesByComponentType) {
        fetchFlowComponents(flow)
            .forEach(extension -> {
                Set<String> sensitivePropertyNames = sensitivePropertiesByComponentType.getOrDefault(extension.getType(), Set.of());
                Map<String, String> encryptedProperties = ofNullable(extension.getProperties()).orElse(Map.of())
                    .entrySet()
                    .stream()
                    .collect(toMap(Entry::getKey, encryptPropertyIfNeeded(sensitivePropertyNames)));
                extension.setProperties(encryptedProperties);
            });
    }

    private Stream<? extends VersionedConfigurableExtension> fetchFlowComponents(VersionedDataflow flow) {
        return concat(
            ofNullable(flow.getControllerServices()).orElse(List.of()).stream(),
            fetchComponentsRecursively(flow.getRootGroup())
        );
    }

    private Stream<? extends VersionedConfigurableExtension> fetchComponentsRecursively(VersionedProcessGroup processGroup) {
        return concat(
            Stream.of(
                    ofNullable(processGroup.getProcessors()).orElse(Set.of()),
                    ofNullable(processGroup.getControllerServices()).orElse(Set.of()))
                .flatMap(Set::stream),
            ofNullable(processGroup.getProcessGroups()).orElse(Set.of()).stream()
                .flatMap(this::fetchComponentsRecursively)
        );
    }

    private Set<String> mergeSets(Set<String> first, Set<String> second) {
        first.addAll(second);
        return first;
    }

    private Function<Entry<String, String>, String> encryptPropertyIfNeeded(Set<String> sensitivePropertyNames) {
        return entry ->
            sensitivePropertyNames.contains(entry.getKey()) && !entry.getValue().startsWith(FlowSerializer.ENC_PREFIX)
                ? encrypt(entry.getValue())
                : entry.getValue();
    }

    private String encrypt(String parameter) {
        return String.format(ENCRYPTED_FORMAT, propertyEncryptor.encrypt(parameter));
    }


}
