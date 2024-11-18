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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AuthenticationConfigurationDTO;
import org.apache.nifi.web.api.entity.AuthenticationConfigurationEntity;
import org.apache.nifi.web.configuration.AuthenticationConfiguration;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.util.Objects;

@Controller
@Path("/authentication")
@Tag(name = "Authentication")
public class AuthenticationResource extends ApplicationResource {
    private final AuthenticationConfiguration authenticationConfiguration;

    public AuthenticationResource(
            final AuthenticationConfiguration authenticationConfiguration,
            final NiFiProperties properties,
            @Autowired(required = false) final RequestReplicator requestReplicator,
            @Autowired(required = false) final ClusterCoordinator clusterCoordinator,
            @Autowired(required = false) final FlowController flowController
    ) {
        this.authenticationConfiguration = Objects.requireNonNull(authenticationConfiguration);
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
        setFlowController(flowController);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/configuration")
    @Operation(
            summary = "Retrieves the authentication configuration endpoint and status information",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = AuthenticationConfigurationEntity.class)))
    )
    public Response getAuthenticationConfiguration() {
        final AuthenticationConfigurationDTO configuration = new AuthenticationConfigurationDTO();
        configuration.setExternalLoginRequired(authenticationConfiguration.externalLoginRequired());
        configuration.setLoginSupported(authenticationConfiguration.loginSupported());

        final URI configuredLoginUri = authenticationConfiguration.loginUri();
        if (configuredLoginUri != null) {
            final String loginUri = getAuthenticationUri(configuredLoginUri);
            configuration.setLoginUri(loginUri);
        }

        final URI configuredLogoutUri = authenticationConfiguration.logoutUri();
        if (configuredLogoutUri != null) {
            final String logoutUri = getAuthenticationUri(configuredLogoutUri);
            configuration.setLogoutUri(logoutUri);
        }

        final AuthenticationConfigurationEntity entity = new AuthenticationConfigurationEntity();
        entity.setAuthenticationConfiguration(configuration);

        return generateOkResponse(entity).build();
    }

    private String getAuthenticationUri(final URI configuredUri) {
        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest);
        builder.path(configuredUri.getPath());

        final String fragment = configuredUri.getFragment();
        if (StringUtils.hasText(fragment)) {
            builder.fragment(fragment);
        }

        return builder.build().toString();
    }
}
