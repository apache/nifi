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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.security.saml.impl.http.HttpServletRequestWithParameters;
import org.apache.nifi.web.security.saml.impl.http.ProxyAwareHttpServletRequestWrapper;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;
import org.opensaml.ws.transport.http.HttpServletResponseAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.context.SAMLMessageContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Implementation of NiFiSAMLContextProvider that inherits from the standard SAMLContextProviderImpl.
 */
public class NiFiSAMLContextProviderImpl extends SAMLContextProviderImpl implements NiFiSAMLContextProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(NiFiSAMLContextProviderImpl.class);

    @Override
    public SAMLMessageContext getLocalEntity(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters)
            throws MetadataProviderException {

        SAMLMessageContext context = new SAMLMessageContext();
        populateGenericContext(request, response, parameters, context);
        populateLocalEntityId(context, request.getRequestURI());
        populateLocalContext(context);
        return context;
    }

    @Override
    public SAMLMessageContext getLocalAndPeerEntity(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters)
            throws MetadataProviderException {

        SAMLMessageContext context = new SAMLMessageContext();
        populateGenericContext(request, response, parameters, context);
        populateLocalEntityId(context, request.getRequestURI());
        populateLocalContext(context);
        populatePeerEntityId(context);
        populatePeerContext(context);
        return context;
    }

    protected void populateGenericContext(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters, SAMLMessageContext context) {
        HttpServletRequestWrapper requestWrapper = new ProxyAwareHttpServletRequestWrapper(request);
        LOGGER.debug("Populating SAMLContext - request wrapper URL is [{}]", requestWrapper.getRequestURL().toString());

        HttpServletRequestAdapter inTransport = new HttpServletRequestWithParameters(requestWrapper, parameters);
        HttpServletResponseAdapter outTransport = new HttpServletResponseAdapter(response, requestWrapper.isSecure());

        // Store attribute which cannot be located from InTransport directly
        requestWrapper.setAttribute(org.springframework.security.saml.SAMLConstants.LOCAL_CONTEXT_PATH, requestWrapper.getContextPath());

        context.setMetadataProvider(metadata);
        context.setInboundMessageTransport(inTransport);
        context.setOutboundMessageTransport(outTransport);

        context.setMessageStorage(storageFactory.getMessageStorage(requestWrapper));
    }

}
