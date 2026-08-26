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
package org.apache.nifi.web.api;

import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.apache.nifi.web.util.ParameterUpdateManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParameterContextResourceTest {

    private static final String TARGET_CONTEXT_ID = "target-context";
    private static final String CURRENT_INHERITED_CONTEXT_ID = "current-inherited-context";
    private static final String REQUESTED_INHERITED_CONTEXT_ID = "requested-inherited-context";

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private ParameterContext targetContext;

    @Mock
    private ParameterContext currentInheritedContext;

    @Mock
    private ParameterContext requestedInheritedContext;

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testAuthorizeReadWriteParameterContextWithComponentsIncludesAffectedComponentsAndInheritedContexts() throws Exception {
        final NiFiUser user = authenticate();

        final ParameterContextResource resource = new ParameterContextResource();
        resource.setServiceFacade(serviceFacade);
        resource.setAuthorizer(authorizer);

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setEntityFactory(new EntityFactory());

        final ParameterUpdateManager parameterUpdateManager = spy(new ParameterUpdateManager(serviceFacade, dtoFactory, authorizer, resource));
        setField(resource, "parameterUpdateManager", parameterUpdateManager);

        final Set<AffectedComponentEntity> affectedComponents = new LinkedHashSet<>();
        final AffectedComponentEntity processor = createAffectedComponent("processor-id", AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        final AffectedComponentEntity controllerService = createAffectedComponent("controller-service-id", AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
        affectedComponents.add(processor);
        affectedComponents.add(controllerService);

        doNothing().when(parameterUpdateManager).authorizeAffectedComponent(any(AffectedComponentEntity.class), same(lookup), same(user), eq(true), eq(true));

        when(targetContext.getInheritedParameterContexts()).thenReturn(List.of(currentInheritedContext));
        when(lookup.getParameterContext(TARGET_CONTEXT_ID)).thenReturn(targetContext);
        when(lookup.getParameterContext(REQUESTED_INHERITED_CONTEXT_ID)).thenReturn(requestedInheritedContext);

        doAnswer(invocation -> {
            final AuthorizeAccess authorizeAccess = invocation.getArgument(0);
            authorizeAccess.authorize(lookup);
            return null;
        }).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        final ParameterContextEntity requestEntity = new ParameterContextEntity();
        requestEntity.setId(TARGET_CONTEXT_ID);
        requestEntity.setRevision(new RevisionDTO());
        requestEntity.setComponent(createRequestParameterContextDto());

        final Method authorizeMethod = ParameterContextResource.class.getDeclaredMethod(
                "authorizeReadWriteParameterContextWithComponents",
                AuthorizableLookup.class,
                String.class,
                ParameterContextEntity.class,
                Set.class,
                NiFiUser.class
        );
        authorizeMethod.setAccessible(true);
        authorizeMethod.invoke(resource, lookup, TARGET_CONTEXT_ID, requestEntity, affectedComponents, user);

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(targetContext).authorize(authorizer, RequestAction.READ, user);
        verify(targetContext).authorize(authorizer, RequestAction.WRITE, user);
        verify(parameterUpdateManager).authorizeAffectedComponent(same(processor), same(lookup), same(user), eq(true), eq(true));
        verify(parameterUpdateManager).authorizeAffectedComponent(same(controllerService), same(lookup), same(user), eq(true), eq(true));
        verify(currentInheritedContext).authorize(authorizer, RequestAction.READ, user);
        verify(requestedInheritedContext).authorize(authorizer, RequestAction.READ, user);
    }

    private static ParameterContextDTO createRequestParameterContextDto() {
        final ParameterContextReferenceEntity requestedInheritedReference = new ParameterContextReferenceEntity();
        requestedInheritedReference.setId(REQUESTED_INHERITED_CONTEXT_ID);

        final ParameterContextDTO dto = new ParameterContextDTO();
        dto.setId(TARGET_CONTEXT_ID);
        dto.setParameters(Set.of());
        dto.setInheritedParameterContexts(List.of(requestedInheritedReference));
        return dto;
    }

    private static AffectedComponentEntity createAffectedComponent(final String componentId, final String referenceType) {
        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(componentId);
        dto.setReferenceType(referenceType);

        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setId(componentId);
        entity.setComponent(dto);
        return entity;
    }

    private static NiFiUser authenticate() {
        final NiFiUser user = new StandardNiFiUser.Builder().identity("unit-test-user").build();
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(user));
        SecurityContextHolder.getContext().setAuthentication(authentication);
        return user;
    }

    private static void setField(final Object target, final String fieldName, final Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
