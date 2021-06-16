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
package org.apache.nifi.registry.web;

import org.apache.nifi.registry.web.api.AccessPolicyResource;
import org.apache.nifi.registry.web.api.AccessResource;
import org.apache.nifi.registry.web.api.BucketBundleResource;
import org.apache.nifi.registry.web.api.BucketFlowResource;
import org.apache.nifi.registry.web.api.BucketResource;
import org.apache.nifi.registry.web.api.ConfigResource;
import org.apache.nifi.registry.web.api.ExtensionRepoResource;
import org.apache.nifi.registry.web.api.BundleResource;
import org.apache.nifi.registry.web.api.ExtensionResource;
import org.apache.nifi.registry.web.api.FlowResource;
import org.apache.nifi.registry.web.api.ItemResource;
import org.apache.nifi.registry.web.api.TenantResource;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.HttpMethodOverrideFilter;
import org.glassfish.jersey.servlet.ServletProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

/**
 * This is the main Jersey configuration for the application.
 *
 *  NOTE: Don't set @ApplicationPath here because it has already been set to 'nifi-registry-api' in JettyServer
 */
@Configuration
public class NiFiRegistryResourceConfig extends ResourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryResourceConfig.class);

    public NiFiRegistryResourceConfig(@Context ServletContext servletContext) {
        // register filters
        register(HttpMethodOverrideFilter.class);

        // register the exception mappers & jackson object mapper resolver
        packages("org.apache.nifi.registry.web.mapper");

        // register endpoints
        register(AccessPolicyResource.class);
        register(AccessResource.class);
        register(BucketResource.class);
        register(BucketFlowResource.class);
        register(BucketBundleResource.class);
        register(BundleResource.class);
        register(ExtensionResource.class);
        register(ExtensionRepoResource.class);
        register(FlowResource.class);
        register(ItemResource.class);
        register(TenantResource.class);
        register(ConfigResource.class);

        // register multipart feature
        register(MultiPartFeature.class);

        // include bean validation errors in response
        property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        // this is necessary for the /access/token/kerberos endpoint to work correctly
        // when sending 401 Unauthorized with a WWW-Authenticate: Negotiate header.
        // if this value needs to be changed, kerberos authentication needs to move to filter chain
        // so it can directly set the HttpServletResponse instead of indirectly through a JAX-RS Response
        property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, true);

        // configure jersey to ignore resource paths for actuator and swagger-ui
        property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "/(actuator|swagger/).*");
    }

}
