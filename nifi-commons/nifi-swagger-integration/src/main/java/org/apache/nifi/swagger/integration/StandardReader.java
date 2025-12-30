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
package org.apache.nifi.swagger.integration;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import io.swagger.v3.jaxrs2.Reader;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.servers.Server;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Standard JAX-RS Method Annotation Reader supporting custom Security Requirement handling
 */
public class StandardReader extends Reader {

    @Override
    protected Operation parseMethod(
            final Class<?> resourceClass,
            final Method method,
            final List<Parameter> parameters,
            final Produces methodProduces,
            final Produces classProduces,
            final Consumes methodConsumes,
            final Consumes classConsumes,
            final List<SecurityRequirement> classSecurityRequirements,
            final Optional<ExternalDocumentation> classExternalDocumentation,
            final Set<String> classTags,
            final List<Server> classServers,
            final boolean isSubresource,
            final RequestBody parentRequestBody,
            final ApiResponses apiResponses,
            final JsonView jsonViewAnnotation,
            final ApiResponse[] classResponses,
            final AnnotatedMethod annotatedMethod
    ) {
        final Operation operation = super.parseMethod(
                resourceClass,
                method,
                parameters,
                methodProduces,
                classProduces,
                methodConsumes,
                classConsumes,
                classSecurityRequirements,
                classExternalDocumentation,
                classTags,
                classServers,
                isSubresource,
                parentRequestBody,
                apiResponses,
                jsonViewAnnotation,
                classResponses,
                annotatedMethod
        );

        // Search for empty SecurityRequirements annotation on method
        final SecurityRequirements[] securityRequirements = method.getAnnotationsByType(SecurityRequirements.class);
        if (securityRequirements.length == 1) {
            final SecurityRequirements requirements = securityRequirements[0];
            if (requirements.value().length == 0) {
                // Set empty Security element on Operation indicated no authentication required
                operation.setSecurity(List.of());
            }
        }

        return operation;
    }
}
