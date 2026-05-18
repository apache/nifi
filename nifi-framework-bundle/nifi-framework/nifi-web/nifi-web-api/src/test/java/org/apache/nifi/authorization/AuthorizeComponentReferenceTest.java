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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeComponentReferenceTest {
    private static final String COMPONENT_TYPE = ComponentAuthorizable.class.getName();

    private static final BundleDTO COMPONENT_BUNDLE = new BundleDTO();

    private static final String CONTROLLER_SERVICE_PROPERTY = "Controller Service";

    private static final String CONTROLLER_SERVICE_ID = "controller-service-id";

    private static final String PARAMETER_PROPERTY = "text";

    private static final String PARAMETER_REFERENCE = "#{param}";

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup authorizableLookup;

    @Mock
    private ComponentAuthorizable componentAuthorizable;

    @Mock
    private Authorizable parameterContext;

    @Mock
    private PropertyDescriptor controllerServicePropertyDescriptor;

    @Mock
    private PropertyDescriptor nonControllerServicePropertyDescriptor;

    @Mock
    private ComponentAuthorizable referencedControllerService;

    @Mock
    private Authorizable referencedControllerServiceAuthorizable;

    @Test
    void testAuthorizeComponentConfigurationComponentType() {
        when(authorizableLookup.getConfigurableComponent(eq(COMPONENT_TYPE), eq(COMPONENT_BUNDLE))).thenReturn(componentAuthorizable);

        AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, authorizableLookup, COMPONENT_TYPE, COMPONENT_BUNDLE, Map.of(), parameterContext);

        verify(componentAuthorizable).cleanUpResources();
    }

    @Test
    void testAuthorizeComponentConfigurationComponentAuthorizable() {
        AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, authorizableLookup, componentAuthorizable, Map.of(), parameterContext);

        verify(componentAuthorizable, never()).cleanUpResources();
    }

    @Test
    void testAuthorizeComponentConfigurationDeniedReferencedControllerService() {
        doReturn(MockControllerService.class).when(controllerServicePropertyDescriptor).getControllerServiceDefinition();
        when(componentAuthorizable.getPropertyDescriptor(eq(CONTROLLER_SERVICE_PROPERTY))).thenReturn(controllerServicePropertyDescriptor);
        when(componentAuthorizable.getValue(eq(controllerServicePropertyDescriptor))).thenReturn(null);
        when(authorizableLookup.getControllerService(eq(CONTROLLER_SERVICE_ID))).thenReturn(referencedControllerService);
        when(referencedControllerService.getAuthorizable()).thenReturn(referencedControllerServiceAuthorizable);
        doThrow(new AccessDeniedException("denied")).when(referencedControllerServiceAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), any());

        final Map<String, String> properties = Map.of(CONTROLLER_SERVICE_PROPERTY, CONTROLLER_SERVICE_ID);

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, authorizableLookup, componentAuthorizable, properties, parameterContext));
    }

    @Test
    void testAuthorizeComponentConfigurationDeniedParameterContextReference() {
        when(componentAuthorizable.getPropertyDescriptor(eq(PARAMETER_PROPERTY))).thenReturn(nonControllerServicePropertyDescriptor);
        doThrow(new AccessDeniedException("denied")).when(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), any());

        final Map<String, String> properties = Map.of(PARAMETER_PROPERTY, PARAMETER_REFERENCE);

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, authorizableLookup, componentAuthorizable, properties, parameterContext));
    }

    private static class MockControllerService extends AbstractControllerService {

    }
}
