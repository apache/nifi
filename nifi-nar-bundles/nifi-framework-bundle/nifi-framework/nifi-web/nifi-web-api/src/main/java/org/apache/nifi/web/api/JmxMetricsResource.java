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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.JmxMetricsResultDTO;
import org.apache.nifi.web.api.entity.JmxMetricsResultsEntity;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsService;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;

/**
 * RESTful endpoint for JMX metrics.
 */
@Path("/jmx-metrics")
@Api(
        value = "/jmx-metrics",
        description = "Endpoint for accessing the JMX metrics."
)
public class JmxMetricsResource extends ApplicationResource {
    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private JmxMetricsService jmxMetricsService;

    /**
     * Retrieves the JMX metrics.
     *
     * @return A jmxMetricsResult list.
     */
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Gets all available JMX metrics",
            response = JmxMetricsResultsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /flow"),
                    @Authorization(value = "Read - /system")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getJmxMetrics(
            @ApiParam(
                    value = "Regular Expression Pattern to be applied against the ObjectName")
            @QueryParam("beanNameFilter") final String beanNameFilter

    ) {
        authorizeJmxMetrics();

        final Collection<JmxMetricsResultDTO> results = jmxMetricsService.getFilteredMBeanMetrics(beanNameFilter);
        final JmxMetricsResultsEntity entity = new JmxMetricsResultsEntity();
        entity.setJmxMetricsResults(results);

        return generateOkResponse(entity)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

    /**
     * Authorizes access to the JMX metrics.
     */
    private void authorizeJmxMetrics() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable system = lookup.getSystem();
            system.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());

            final Authorizable flow = lookup.getFlow();
            flow.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setJmxMetricsService(final JmxMetricsService jmxMetricsService) {
        this.jmxMetricsService = jmxMetricsService;
    }
}
