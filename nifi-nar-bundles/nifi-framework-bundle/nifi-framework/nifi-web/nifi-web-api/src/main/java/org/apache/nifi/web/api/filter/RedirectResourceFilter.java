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


import org.apache.nifi.web.api.SiteToSiteResource;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;

/**
 * This filter provides backward compatibility for Resource URI changes.
 */
@PreMatching
public class RedirectResourceFilter implements ContainerRequestFilter {

    /**
     * This method checks path of the incoming request, and
     * redirects following URIs:
     * <li>/controller -> SiteToSiteResource
     * @param requestContext request to be modified
     */
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        final UriInfo uriInfo = requestContext.getUriInfo();

        if (uriInfo.getPath().equals("controller")){
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri())
                    .path(SiteToSiteResource.class)
                    .replaceQuery(uriInfo.getRequestUri().getRawQuery());

            URI redirectTo = builder.build();
            requestContext.setRequestUri(uriInfo.getBaseUri(), redirectTo);
        }
    }
}
