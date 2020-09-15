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
package org.apache.nifi.web.security.saml;

import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.springframework.security.saml.context.SAMLMessageContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Specialized interface to add functionality to {@link org.springframework.security.saml.context.SAMLContextProvider}
 */
public interface NiFiSAMLContextProvider {

    /**
     * Creates a SAMLContext with local entity values filled. Also request and response must be stored in the context
     * as message transports. Local entity ID is populated from data in the request object.
     *
     * @param request request
     * @param response response
     * @param parameters additional parameters
     * @return context
     * @throws MetadataProviderException in case of metadata problems
     */
    SAMLMessageContext getLocalEntity(HttpServletRequest request, HttpServletResponse response, Map<String,String> parameters)
            throws MetadataProviderException;

    /**
     * Creates a SAMLContext with local entity and peer values filled. Also request and response must be stored in the context
     * as message transports. Local and peer entity IDs are populated from data in the request object.
     *
     * @param request request
     * @param response response
     * @param parameters additional parameters
     * @return context
     * @throws MetadataProviderException in case of metadata problems
     */
    SAMLMessageContext getLocalAndPeerEntity(HttpServletRequest request, HttpServletResponse response, Map<String,String> parameters)
            throws MetadataProviderException;

}
