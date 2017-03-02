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
package org.apache.nifi.web.api.filter;


import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import org.apache.nifi.web.api.SiteToSiteResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * This filter provides backward compatibility for Resource URI changes.
 */
public class RedirectResourceFilter implements ContainerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(RedirectResourceFilter.class);

    /**
     * This method checks path of the incoming request, and
     * redirects following URIs:
     * <li>/controller -> SiteToSiteResource
     * @param containerRequest request to be modified
     * @return modified request
     */
    @Override
    public ContainerRequest filter(ContainerRequest containerRequest) {

        if(containerRequest.getPath().equals("controller")){
            UriBuilder builder = UriBuilder.fromUri(containerRequest.getBaseUri())
                    .path(SiteToSiteResource.class)
                    .replaceQuery(containerRequest.getRequestUri().getRawQuery());

            URI redirectTo = builder.build();
            containerRequest.setUris(containerRequest.getBaseUri(), redirectTo);
        }
        return containerRequest;
    }
}
