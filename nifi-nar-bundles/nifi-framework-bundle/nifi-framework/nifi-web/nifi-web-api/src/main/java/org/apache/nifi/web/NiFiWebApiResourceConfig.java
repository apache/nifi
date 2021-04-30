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
package org.apache.nifi.web;

import org.apache.nifi.web.api.config.AccessDeniedExceptionMapper;
import org.apache.nifi.web.api.config.AdministrationExceptionMapper;
import org.apache.nifi.web.api.config.AuthenticationCredentialsNotFoundExceptionMapper;
import org.apache.nifi.web.api.config.AuthenticationNotSupportedExceptionMapper;
import org.apache.nifi.web.api.config.AuthorizationAccessExceptionMapper;
import org.apache.nifi.web.api.config.ClusterExceptionMapper;
import org.apache.nifi.web.api.config.IllegalArgumentExceptionMapper;
import org.apache.nifi.web.api.config.IllegalClusterResourceRequestExceptionMapper;
import org.apache.nifi.web.api.config.IllegalClusterStateExceptionMapper;
import org.apache.nifi.web.api.config.IllegalNodeDeletionExceptionMapper;
import org.apache.nifi.web.api.config.IllegalNodeDisconnectionExceptionMapper;
import org.apache.nifi.web.api.config.IllegalNodeReconnectionExceptionMapper;
import org.apache.nifi.web.api.config.IllegalStateExceptionMapper;
import org.apache.nifi.web.api.config.InvalidAuthenticationExceptionMapper;
import org.apache.nifi.web.api.config.InvalidRevisionExceptionMapper;
import org.apache.nifi.web.api.config.JsonContentConversionExceptionMapper;
import org.apache.nifi.web.api.config.JsonMappingExceptionMapper;
import org.apache.nifi.web.api.config.JsonParseExceptionMapper;
import org.apache.nifi.web.api.config.MutableRequestExceptionMapper;
import org.apache.nifi.web.api.config.NiFiCoreExceptionMapper;
import org.apache.nifi.web.api.config.NoClusterCoordinatorExceptionMapper;
import org.apache.nifi.web.api.config.NoConnectedNodesExceptionMapper;
import org.apache.nifi.web.api.config.NoResponseFromNodesExceptionMapper;
import org.apache.nifi.web.api.config.NodeDisconnectionExceptionMapper;
import org.apache.nifi.web.api.config.NodeReconnectionExceptionMapper;
import org.apache.nifi.web.api.config.NotFoundExceptionMapper;
import org.apache.nifi.web.api.config.ResourceNotFoundExceptionMapper;
import org.apache.nifi.web.api.config.ThrowableMapper;
import org.apache.nifi.web.api.config.UnknownNodeExceptionMapper;
import org.apache.nifi.web.api.config.ValidationExceptionMapper;
import org.apache.nifi.web.api.config.WebApplicationExceptionMapper;
import org.apache.nifi.web.api.filter.RedirectResourceFilter;
import org.apache.nifi.web.util.ObjectMapperResolver;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class NiFiWebApiResourceConfig extends ResourceConfig {

    public NiFiWebApiResourceConfig(@Context ServletContext servletContext) {
        // get the application context to register the rest endpoints
        final ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(servletContext);

        // request support
        register(RedirectResourceFilter.class);
        register(MultiPartFeature.class);

        // jackson
        register(JacksonFeature.class);
        register(ObjectMapperResolver.class);

        // rest api
        register(ctx.getBean("flowResource"));
        register(ctx.getBean("resourceResource"));
        register(ctx.getBean("controllerResource"));
        register(ctx.getBean("siteToSiteResource"));
        register(ctx.getBean("dataTransferResource"));
        register(ctx.getBean("snippetResource"));
        register(ctx.getBean("templateResource"));
        register(ctx.getBean("controllerServiceResource"));
        register(ctx.getBean("reportingTaskResource"));
        register(ctx.getBean("processGroupResource"));
        register(ctx.getBean("processorResource"));
        register(ctx.getBean("connectionResource"));
        register(ctx.getBean("flowfileQueueResource"));
        register(ctx.getBean("remoteProcessGroupResource"));
        register(ctx.getBean("inputPortResource"));
        register(ctx.getBean("outputPortResource"));
        register(ctx.getBean("labelResource"));
        register(ctx.getBean("funnelResource"));
        register(ctx.getBean("provenanceResource"));
        register(ctx.getBean("provenanceEventResource"));
        register(ctx.getBean("countersResource"));
        register(ctx.getBean("systemDiagnosticsResource"));
        register(ctx.getBean("accessResource"));
        register(ctx.getBean("samlResource"));
        register(ctx.getBean("oidcResource"));
        register(ctx.getBean("accessPolicyResource"));
        register(ctx.getBean("tenantsResource"));
        register(ctx.getBean("versionsResource"));
        register(ctx.getBean("parameterContextResource"));

        // exception mappers
        register(AccessDeniedExceptionMapper.class);
        register(AuthorizationAccessExceptionMapper.class);
        register(AuthenticationNotSupportedExceptionMapper.class);
        register(InvalidAuthenticationExceptionMapper.class);
        register(AuthenticationCredentialsNotFoundExceptionMapper.class);
        register(AdministrationExceptionMapper.class);
        register(ClusterExceptionMapper.class);
        register(IllegalArgumentExceptionMapper.class);
        register(IllegalClusterResourceRequestExceptionMapper.class);
        register(IllegalClusterStateExceptionMapper.class);
        register(IllegalNodeDeletionExceptionMapper.class);
        register(IllegalNodeDisconnectionExceptionMapper.class);
        register(IllegalNodeReconnectionExceptionMapper.class);
        register(IllegalStateExceptionMapper.class);
        register(InvalidRevisionExceptionMapper.class);
        register(JsonMappingExceptionMapper.class);
        register(JsonParseExceptionMapper.class);
        register(JsonContentConversionExceptionMapper.class);
        register(MutableRequestExceptionMapper.class);
        register(NiFiCoreExceptionMapper.class);
        register(NoConnectedNodesExceptionMapper.class);
        register(NoClusterCoordinatorExceptionMapper.class);
        register(NoResponseFromNodesExceptionMapper.class);
        register(NodeDisconnectionExceptionMapper.class);
        register(NodeReconnectionExceptionMapper.class);
        register(ResourceNotFoundExceptionMapper.class);
        register(NotFoundExceptionMapper.class);
        register(UnknownNodeExceptionMapper.class);
        register(ValidationExceptionMapper.class);
        register(WebApplicationExceptionMapper.class);
        register(ThrowableMapper.class);

        // gzip
        EncodingFilter.enableFor(this, GZipEncoder.class);
    }

}
