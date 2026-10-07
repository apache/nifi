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
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeParameterProviderApplyTest {

    private static final String PARAMETER_PROVIDER_ID = "parameter-provider-id";

    private static final String PARAMETER_CONTEXT_ID = "parameter-context-id";

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private NiFiUser user;

    @Mock
    private ComponentAuthorizable parameterProviderAuthorizable;

    @Mock
    private Authorizable parameterProviderResource;

    @Mock
    private Authorizable parameterContexts;

    @Mock
    private ParameterContext parameterContext;

    @Test
    void testAuthorizeApplyParametersRequiresWriteToCreateParameterContext() {
        when(lookup.getParameterProvider(PARAMETER_PROVIDER_ID)).thenReturn(parameterProviderAuthorizable);
        when(parameterProviderAuthorizable.getAuthorizable()).thenReturn(parameterProviderResource);
        when(lookup.getParameterContexts()).thenReturn(parameterContexts);
        doThrow(new AccessDeniedException("Unable to modify parameter contexts")).when(parameterContexts)
                .authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));

        assertThrows(AccessDeniedException.class, () -> AuthorizeParameterProviderApply.authorizeApplyParameters(
                PARAMETER_PROVIDER_ID, true, List.of(), authorizer, lookup, user));

        verify(parameterProviderResource).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContexts).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
    }

    @Test
    void testAuthorizeApplyParametersDoesNotRequireWriteToCreateWhenNoNewParameterContext() {
        when(lookup.getParameterProvider(PARAMETER_PROVIDER_ID)).thenReturn(parameterProviderAuthorizable);
        when(parameterProviderAuthorizable.getAuthorizable()).thenReturn(parameterProviderResource);

        AuthorizeParameterProviderApply.authorizeApplyParameters(PARAMETER_PROVIDER_ID, false, List.of(), authorizer, lookup, user);

        verify(parameterProviderResource).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(lookup, never()).getParameterContexts();
    }

    @Test
    void testAuthorizeApplyParametersRequiresReadAndWriteOnExistingParameterContext() {
        when(lookup.getParameterProvider(PARAMETER_PROVIDER_ID)).thenReturn(parameterProviderAuthorizable);
        when(parameterProviderAuthorizable.getAuthorizable()).thenReturn(parameterProviderResource);
        when(lookup.getParameterContext(PARAMETER_CONTEXT_ID)).thenReturn(parameterContext);
        doNothing().when(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        doThrow(new AccessDeniedException("Unable to write Parameter Context")).when(parameterContext)
                .authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));

        assertThrows(AccessDeniedException.class, () -> AuthorizeParameterProviderApply.authorizeApplyParameters(
                PARAMETER_PROVIDER_ID, false, List.of(parameterContextEntity()), authorizer, lookup, user));

        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.READ), eq(user));
        verify(parameterContext).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));
        verify(lookup, never()).getParameterContexts();
    }

    private static ParameterContextEntity parameterContextEntity() {
        final ParameterContextDTO parameterContextDto = new ParameterContextDTO();
        parameterContextDto.setId(PARAMETER_CONTEXT_ID);

        final ParameterContextEntity parameterContextEntity = new ParameterContextEntity();
        parameterContextEntity.setComponent(parameterContextDto);
        return parameterContextEntity;
    }
}
