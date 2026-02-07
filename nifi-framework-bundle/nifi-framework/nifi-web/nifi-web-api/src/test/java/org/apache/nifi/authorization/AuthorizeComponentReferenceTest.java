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
import org.apache.nifi.web.api.dto.BundleDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeComponentReferenceTest {
    private static final String COMPONENT_TYPE = ComponentAuthorizable.class.getName();

    private static final BundleDTO COMPONENT_BUNDLE = new BundleDTO();

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup authorizableLookup;

    @Mock
    private ComponentAuthorizable componentAuthorizable;

    @Mock
    private Authorizable restrictedAuthorizable;

    @Mock
    private Authorizable parameterContext;

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
    void testAuthorizeComponentConfigurationRestricted() {
        when(componentAuthorizable.isRestricted()).thenReturn(true);
        when(componentAuthorizable.getRestrictedAuthorizables()).thenReturn(Set.of(restrictedAuthorizable));

        AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, authorizableLookup, componentAuthorizable, null, null);

        verify(restrictedAuthorizable).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
        verify(componentAuthorizable, never()).cleanUpResources();
    }
}
