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

package org.apache.nifi.web.standard.api.component;

import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebConfigurationRequestContext;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.standard.api.AbstractStandardResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/standard/reporting-task")
public class ReportingTaskResource extends AbstractStandardResource {

    private static final Logger logger = LoggerFactory.getLogger(ReportingTaskResource.class);

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/details")
    public Response getDetails(@QueryParam("reportingTaskId") final String reportingTaskId) {
        final NiFiWebConfigurationContext nifiWebContext = getWebConfigurationContext();
        final ComponentDetails componentDetails = ComponentWebUtils.getComponentDetails(nifiWebContext, UiExtensionType.ReportingTaskConfiguration, reportingTaskId, request);
        final Response.ResponseBuilder response = ComponentWebUtils.applyCacheControl(Response.ok(componentDetails));
        return response.build();
    }

    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes({MediaType.APPLICATION_JSON})
    @Path("/properties")
    public Response setProperties(@QueryParam("reportingTaskId") final String reportingTaskId, @QueryParam("revisionId") final Long revisionId,
                                  @QueryParam("clientId") final String clientId, Map<String, String> properties) {
        final NiFiWebConfigurationContext nifiWebContext = getWebConfigurationContext();
        final NiFiWebConfigurationRequestContext nifiRequestContext = ComponentWebUtils.getRequestContext(UiExtensionType.ReportingTaskConfiguration, reportingTaskId, revisionId, clientId, request);
        final ComponentDetails componentDetails = nifiWebContext.updateComponent(nifiRequestContext, null, properties);
        final Response.ResponseBuilder response = ComponentWebUtils.applyCacheControl(Response.ok(componentDetails));
        return response.build();
    }

}
