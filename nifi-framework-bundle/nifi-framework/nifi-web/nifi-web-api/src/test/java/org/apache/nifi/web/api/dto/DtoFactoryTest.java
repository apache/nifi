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
package org.apache.nifi.web.api.dto;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DtoFactoryTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    void testAllowableValuesControllerService() {
        final Set<String> csIdentifiers = new HashSet<>(Arrays.asList("uuid-2", "uuid-3", "uuid-1"));
        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(controllerServiceProvider.getControllerServiceIdentifiers(any(), any())).thenReturn(csIdentifiers);
        final ControllerServiceNode service1 = mock(ControllerServiceNode.class);
        final ControllerServiceNode service2 = mock(ControllerServiceNode.class);
        final ControllerServiceNode service3 = mock(ControllerServiceNode.class);
        when(service1.getIdentifier()).thenReturn("uuid-1");
        when(service2.getIdentifier()).thenReturn("uuid-2");
        when(service3.getIdentifier()).thenReturn("uuid-3");
        when(service1.getName()).thenReturn("ReaderZ");
        when(service2.getName()).thenReturn("ReaderY");
        when(service3.getName()).thenReturn("ReaderX");
        when(service1.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(service2.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(service3.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-1"))).thenReturn(service1);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-2"))).thenReturn(service2);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-3"))).thenReturn(service3);

        final DtoFactory dtoFactory = new DtoFactory();
        final EntityFactory entityFactory = new EntityFactory();
        dtoFactory.setControllerServiceProvider(controllerServiceProvider);
        dtoFactory.setEntityFactory(entityFactory);
        final StandardExtensionDiscoveringManager extensionManager =
                new StandardExtensionDiscoveringManager(Collections.singleton(ControllerService.class));
        extensionManager.discoverExtensions(
                Collections.singleton(SystemBundle.create(".", getClass().getClassLoader())));
        dtoFactory.setExtensionManager(extensionManager);

        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
                .name("reader")
                .identifiesControllerService(ControllerService.class)
                .build();
        final PropertyDescriptorDTO dto = dtoFactory.createPropertyDescriptorDto(propertyDescriptor, null);
        final List<AllowableValueEntity> allowableValues = dto.getAllowableValues();
        final List<String> namesActual = allowableValues.stream()
                .map(v -> v.getAllowableValue().getDisplayName()).collect(Collectors.toList());
        logger.trace(namesActual.toString());
        assertEquals(Arrays.asList("ReaderX", "ReaderY", "ReaderZ"), namesActual);
    }

    @Test
    void testAllowableValuesFixed() {
        // from "org.apache.nifi.processors.aws.s3.AbstractS3Processor"
        final PropertyDescriptor signerOverride = new PropertyDescriptor.Builder()
                .name("Signer Override")
                .allowableValues(
                        new AllowableValue("Default Signature", "Default Signature"),
                        new AllowableValue("AWSS3V4SignerType", "Signature v4"),
                        new AllowableValue("S3SignerType", "Signature v2"))
                .defaultValue("Default Signature")
                .build();

        final DtoFactory dtoFactory = new DtoFactory();
        final EntityFactory entityFactory = new EntityFactory();
        dtoFactory.setEntityFactory(entityFactory);
        final PropertyDescriptorDTO dto = dtoFactory.createPropertyDescriptorDto(signerOverride, null);
        final List<AllowableValueEntity> allowableValues = dto.getAllowableValues();
        final List<String> namesActual = allowableValues.stream()
                .map(v -> v.getAllowableValue().getDisplayName()).collect(Collectors.toList());
        logger.trace(namesActual.toString());
        assertEquals(Arrays.asList("Default Signature", "Signature v4", "Signature v2"), namesActual);
    }
}
