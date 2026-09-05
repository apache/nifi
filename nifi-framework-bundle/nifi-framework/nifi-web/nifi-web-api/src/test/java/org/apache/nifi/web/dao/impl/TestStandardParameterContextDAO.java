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

package org.apache.nifi.web.dao.impl;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestStandardParameterContextDAO {

    private static final String CONTEXT_ID = "id";
    private static final String CONTEXT_NAME = "Context";
    private static final String INHERITED_CONTEXT_ID = "inherited-id";

    private StandardParameterContextDAO dao;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private FlowController flowController;

    @Mock
    private AssetManager assetManager;

    @Mock
    private Authentication authentication;

    @Mock
    private Authorizer authorizer;

    @BeforeEach
    void setUp() {
        dao = new StandardParameterContextDAO();
        when(flowController.getAssetManager()).thenReturn(assetManager);
        dao.setFlowController(flowController);
        dao.setAuthorizer(authorizer);
        lenient().when(authorizer.authorize(any(AuthorizationRequest.class))).thenReturn(AuthorizationResult.approved());

        final SecurityContext securityContext = SecurityContextHolder.getContext();
        securityContext.setAuthentication(authentication);
        final NiFiUser user = new StandardNiFiUser.Builder().identity("user").build();
        final NiFiUserDetails userDetail = new NiFiUserDetails(user);
        lenient().when(authentication.getPrincipal()).thenReturn(userDetail);

        final ParameterReferenceManager parameterReferenceManager = new StandardParameterReferenceManager(() -> flowController.getFlowManager().getRootGroup());

        final FlowManager flowManager = flowController.getFlowManager();
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        when(flowManager.getParameterContextManager()).thenReturn(parameterContextLookup);
        parameterContextLookup.addParameterContext(new StandardParameterContext.Builder().id(CONTEXT_ID)
                .name(CONTEXT_NAME)
                .parameterReferenceManager(parameterReferenceManager)
                .build());
        final ParameterContext inheritedContext = new StandardParameterContext.Builder().id(INHERITED_CONTEXT_ID)
                .parameterReferenceManager(parameterReferenceManager)
                .name("Inherited")
                .build();
        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("inherited-param", new Parameter.Builder().name("inherited-param").value("value").provided(true).build());
        inheritedContext.setParameters(parameters);
        parameterContextLookup.addParameterContext(inheritedContext);
    }

    @Test
    public void testVerifyUpdateInheritedProvidedParameter() {
        final ParameterContextDTO dto = new ParameterContextDTO();
        dto.setId(CONTEXT_ID);
        dto.setName(CONTEXT_NAME);

        final List<ParameterContextReferenceEntity> refs = new ArrayList<>();
        final ParameterContextReferenceEntity ref = new ParameterContextReferenceEntity();
        ref.setId(INHERITED_CONTEXT_ID);
        ref.setComponent(new ParameterContextReferenceDTO());
        ref.getComponent().setId(INHERITED_CONTEXT_ID);
        ref.getComponent().setName("Inherited");
        refs.add(ref);
        dto.setInheritedParameterContexts(refs);

        // Updating a provided parameter that is inherited is allowed
        dao.verifyUpdate(dto, true);
    }

    @Test
    public void testVerifyUpdateNonInheritedProvidedParameter() {
        final ParameterContextDTO dto = new ParameterContextDTO();
        dto.setId(CONTEXT_ID);
        dto.setName(CONTEXT_NAME);
        final Set<ParameterEntity> parameters = new HashSet<>();
        final ParameterEntity parameter = new ParameterEntity();
        parameter.setCanWrite(true);
        final ParameterDTO parameterDto = new ParameterDTO();
        parameterDto.setProvided(true);
        parameterDto.setName("param");
        parameterDto.setValue("value");
        parameterDto.setInherited(false);
        parameter.setParameter(parameterDto);
        parameters.add(parameter);
        dto.setParameters(parameters);

        final List<ParameterContextReferenceEntity> refs = new ArrayList<>();
        final ParameterContextReferenceEntity ref = new ParameterContextReferenceEntity();
        ref.setId(INHERITED_CONTEXT_ID);
        ref.setComponent(new ParameterContextReferenceDTO());
        ref.getComponent().setId(INHERITED_CONTEXT_ID);
        ref.getComponent().setName("Inherited");
        refs.add(ref);
        dto.setInheritedParameterContexts(refs);

        // Updating a provided parameter that is not inherited should fail
        assertThrows(IllegalArgumentException.class, () -> dao.verifyUpdate(dto, true));
    }

    @Test
    public void testGetParametersNormalizesNullSourceForLocalParameter() {
        final ParameterEntity parameterEntity = createParameterEntity("param-null-source", "value-1", false, "description-1", false, null, null);

        final Map<String, Parameter> parameters = dao.getParameters(createParameterContextDto(parameterEntity), null);

        final Parameter parameter = parameters.get("param-null-source");
        assertEquals("value-1", parameter.getValue());
        assertEquals("description-1", parameter.getDescriptor().getDescription());
        assertFalse(parameter.getDescriptor().isSensitive());
        assertFalse(parameter.isProvided());
        assertNull(parameter.getParameterContextId());
    }

    @Test
    public void testGetParametersNormalizesCurrentSourceForExistingLocalParameterUpdate() {
        final ParameterContext context = dao.getParameterContext(CONTEXT_ID);
        context.setParameters(Map.of(
                "param-current-source",
                new Parameter.Builder().name("param-current-source").value("existing-value").build()
        ));

        final ParameterEntity parameterEntity = createParameterEntity("param-current-source", null, true, "description-2", false, CONTEXT_ID, null);

        final Map<String, Parameter> parameters = dao.getParameters(createParameterContextDto(parameterEntity), context);

        final Parameter parameter = parameters.get("param-current-source");
        assertEquals("existing-value", parameter.getValue());
        assertEquals("description-2", parameter.getDescriptor().getDescription());
        assertTrue(parameter.getDescriptor().isSensitive());
        assertFalse(parameter.isProvided());
        assertNull(parameter.getParameterContextId());
    }

    @Test
    public void testGetParametersNormalizesForeignSourceAndPreservesFields() {
        final ParameterEntity parameterEntity = createParameterEntity("param-foreign-source", "value-3", true, "description-3", false, "foreign-context", null);

        final Map<String, Parameter> parameters = dao.getParameters(createParameterContextDto(parameterEntity), null);

        final Parameter parameter = parameters.get("param-foreign-source");
        assertEquals("value-3", parameter.getValue());
        assertEquals("description-3", parameter.getDescriptor().getDescription());
        assertTrue(parameter.getDescriptor().isSensitive());
        assertFalse(parameter.isProvided());
        assertNull(parameter.getParameterContextId());
    }

    @Test
    public void testGetParametersNormalizesForeignSourceForProvidedParameter() {
        final ParameterEntity parameterEntity = createParameterEntity("provided-param", "provided-value", false, "provided-description", true, "foreign-context", null);

        final Map<String, Parameter> parameters = dao.getParameters(createParameterContextDto(parameterEntity), null);

        final Parameter parameter = parameters.get("provided-param");
        assertEquals("provided-value", parameter.getValue());
        assertEquals("provided-description", parameter.getDescriptor().getDescription());
        assertFalse(parameter.getDescriptor().isSensitive());
        assertTrue(parameter.isProvided());
        assertNull(parameter.getParameterContextId());
    }

    @Test
    public void testGetParametersNormalizesForeignSourceForAssetBackedParameterAndPassesOwnershipValidation() {
        final Asset asset = mock(Asset.class);
        when(asset.getOwnerIdentifier()).thenReturn(CONTEXT_ID);
        when(asset.getFile()).thenReturn(new java.io.File("asset.bin"));
        when(assetManager.getAsset("asset-1")).thenReturn(Optional.of(asset));

        final AssetReferenceDTO assetReference = new AssetReferenceDTO();
        assetReference.setId("asset-1");

        final ParameterEntity parameterEntity = createParameterEntity("asset-param", "client-value-ignored", false, "asset-description", false,
                "foreign-context", List.of(assetReference));
        final ParameterContextDTO parameterContextDto = createParameterContextDto(parameterEntity);

        final Map<String, Parameter> parameters = dao.getParameters(parameterContextDto, null);
        final Parameter parameter = parameters.get("asset-param");

        assertEquals(asset.getFile().getAbsolutePath(), parameter.getValue());
        assertEquals("asset-description", parameter.getDescriptor().getDescription());
        assertNull(parameter.getParameterContextId());
        assertEquals(Collections.singletonList(asset), parameter.getReferencedAssets());
        assertDoesNotThrow(() -> dao.verifyAssets(parameterContextDto, parameters));
        verify(assetManager).getAsset(eq("asset-1"));
    }

    private ParameterContextDTO createParameterContextDto(final ParameterEntity... parameterEntities) {
        final ParameterContextDTO parameterContextDto = new ParameterContextDTO();
        parameterContextDto.setId(CONTEXT_ID);
        parameterContextDto.setName(CONTEXT_NAME);
        parameterContextDto.setParameters(new HashSet<>(List.of(parameterEntities)));
        return parameterContextDto;
    }

    private ParameterEntity createParameterEntity(final String name, final String value, final Boolean sensitive, final String description,
                                                  final Boolean provided, final String sourceContextId, final List<AssetReferenceDTO> referencedAssets) {
        final ParameterDTO parameterDto = new ParameterDTO();
        parameterDto.setName(name);
        parameterDto.setValue(value);
        parameterDto.setSensitive(sensitive);
        parameterDto.setDescription(description);
        parameterDto.setProvided(provided);
        parameterDto.setReferencedAssets(referencedAssets);
        if (sourceContextId != null) {
            final ParameterContextReferenceEntity parameterContextReference = new ParameterContextReferenceEntity();
            parameterContextReference.setId(sourceContextId);
            final ParameterContextReferenceDTO parameterContextReferenceDto = new ParameterContextReferenceDTO();
            parameterContextReferenceDto.setId(sourceContextId);
            parameterContextReference.setComponent(parameterContextReferenceDto);
            parameterDto.setParameterContext(parameterContextReference);
        }

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setCanWrite(true);
        parameterEntity.setParameter(parameterDto);
        return parameterEntity;
    }
}
