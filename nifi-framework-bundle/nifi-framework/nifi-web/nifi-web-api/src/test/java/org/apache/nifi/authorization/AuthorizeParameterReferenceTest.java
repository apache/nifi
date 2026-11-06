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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.web.NiFiServiceFacade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeParameterReferenceTest {

    private static final String PARAMETER_CONTEXT_NAME = "Parameter Context";

    private static final String PARAMETER_PROVIDER_ID = "parameter-provider-id";

    private static final String ACCESS_DENIED_MESSAGE = "denied";

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private NiFiUser user;

    @Mock
    private ParameterContext parameterContext;

    @Mock
    private Authorizable parameterContexts;

    @Mock
    private ParameterProvider currentParameterProvider;

    @Mock
    private ComponentAuthorizable parameterProviderComponent;

    @Mock
    private Authorizable parameterProviderAuthorizable;

    @Test
    void testAuthorizeParameterContextAdditionDeniedWhenAttachingProviderWithoutWrite() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        stubExistingParameterContext();
        when(parameterContext.getParameterProvider()).thenReturn(null);
        doNothing().when(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        doThrow(new AccessDeniedException(ACCESS_DENIED_MESSAGE)).when(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user));

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(lookup, never()).getParameterProviders(any());
    }

    @Test
    void testAuthorizeParameterContextAdditionRequiresWriteAndProviderReadWhenAttachingProvider() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        stubExistingParameterContext();
        when(parameterContext.getParameterProvider()).thenReturn(null);
        stubLocalParameterProvider();

        AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user);

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(parameterProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
    }

    @Test
    void testAuthorizeParameterContextAdditionDoesNotRequireWriteWhenProviderAlreadyConfigured() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        stubExistingParameterContext();
        when(parameterContext.getParameterProvider()).thenReturn(currentParameterProvider);

        AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user);

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContext, never()).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(lookup, never()).getParameterProviders(any());
    }

    @Test
    void testAuthorizeParameterContextAdditionDeniedWhenProviderReadDenied() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        stubExistingParameterContext();
        when(parameterContext.getParameterProvider()).thenReturn(null);
        stubLocalParameterProvider();
        doThrow(new AccessDeniedException(ACCESS_DENIED_MESSAGE)).when(parameterProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user));

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(parameterProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
    }

    @Test
    void testAuthorizeParameterContextAdditionRequiresCreateAndProviderReadForNewContext() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        when(serviceFacade.getParameterContextByName(eq(PARAMETER_CONTEXT_NAME), eq(user))).thenReturn(null);
        when(lookup.getParameterContexts()).thenReturn(parameterContexts);
        stubLocalParameterProvider();

        AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user);

        verify(parameterContexts).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(parameterProviderAuthorizable).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
    }

    @Test
    void testAuthorizeParameterContextAdditionDoesNotRequireWriteWhenNoProviderProposed() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(null);
        when(serviceFacade.getParameterContextByName(eq(PARAMETER_CONTEXT_NAME), eq(user))).thenReturn(parameterContext);
        when(parameterContext.getParameters()).thenReturn(Collections.emptyMap());
        when(parameterContext.getInheritedParameterContextNames()).thenReturn(List.of());

        AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user);

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContext, never()).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(lookup, never()).getParameterProviders(any());
    }

    @Test
    void testAuthorizeParameterContextAdditionRequiresWriteWhenProviderIsNotLocal() {
        final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(PARAMETER_PROVIDER_ID);
        stubExistingParameterContext();
        when(parameterContext.getParameterProvider()).thenReturn(null);
        when(lookup.getParameterProviders(any())).thenReturn(Set.of());

        AuthorizeParameterReference.authorizeParameterContextAddition(versionedParameterContext, serviceFacade, authorizer, lookup, user);

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(lookup).getParameterProviders(any());
    }

    private void stubExistingParameterContext() {
        when(serviceFacade.getParameterContextByName(eq(PARAMETER_CONTEXT_NAME), eq(user))).thenReturn(parameterContext);
        when(parameterContext.getParameters()).thenReturn(Collections.emptyMap());
        when(parameterContext.getInheritedParameterContextNames()).thenReturn(List.of());
    }

    private void stubLocalParameterProvider() {
        when(lookup.getParameterProviders(any())).thenReturn(Set.of(parameterProviderComponent));
        when(parameterProviderComponent.getAuthorizable()).thenReturn(parameterProviderAuthorizable);
    }

    private VersionedParameterContext createVersionedParameterContext(final String parameterProviderId) {
        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setName(PARAMETER_CONTEXT_NAME);
        versionedParameterContext.setParameters(Set.of());
        versionedParameterContext.setInheritedParameterContexts(List.of());
        versionedParameterContext.setParameterProvider(parameterProviderId);
        return versionedParameterContext;
    }
}
