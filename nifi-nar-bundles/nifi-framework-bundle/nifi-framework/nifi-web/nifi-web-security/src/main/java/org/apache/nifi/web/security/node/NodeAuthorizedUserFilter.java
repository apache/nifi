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
package org.apache.nifi.web.security.node;

import java.io.IOException;
import java.io.Serializable;
import java.security.cert.X509Certificate;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.nifi.controller.FlowController;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.web.security.x509.SubjectDnX509PrincipalExtractor;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.filter.GenericFilterBean;

/**
 * Custom filter to extract a user's authorities from the request where the user
 * was authenticated by the cluster manager and populate the threadlocal with
 * the authorized user. If the request contains the appropriate header with
 * authorities and the application instance is a node connected to the cluster,
 * then the authentication/authorization steps remaining in the filter chain are
 * skipped.
 *
 * Checking if the application instance is a connected node is important because
 * it prevents external clients from faking the request headers and bypassing
 * the authentication processing chain.
 */
public class NodeAuthorizedUserFilter extends GenericFilterBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeAuthorizedUserFilter.class);

    public static final String PROXY_USER_DETAILS = "X-ProxiedEntityUserDetails";

    private final NiFiProperties properties;
    private final AuthenticationDetailsSource authenticationDetailsSource = new WebAuthenticationDetailsSource();
    private final X509CertificateExtractor certificateExtractor = new X509CertificateExtractor();
    private final X509PrincipalExtractor principalExtractor = new SubjectDnX509PrincipalExtractor();

    public NodeAuthorizedUserFilter(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;

        // get the proxied user's authorities
        final String hexEncodedUserDetails = httpServletRequest.getHeader(PROXY_USER_DETAILS);

        // check if the request has the necessary header information and this instance is configured as a node
        if (StringUtils.isNotBlank(hexEncodedUserDetails) && properties.isNode()) {

            // get the flow controller from the Spring context
            final ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
            final FlowController flowController = ctx.getBean("flowController", FlowController.class);

            // check that we are connected to the cluster
            if (flowController.getNodeId() != null) {
                try {
                    // get the DN from the cert in the request
                    final X509Certificate certificate = certificateExtractor.extractClientCertificate((HttpServletRequest) request);
                    if (certificate != null) {
                        // extract the principal from the certificate
                        final Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
                        final String dn = certificatePrincipal.toString();

                        // only consider the pre-authorized user when the request came from the NCM according to the DN in the certificate
                        final String clusterManagerDN = flowController.getClusterManagerDN();
                        if (clusterManagerDN != null && clusterManagerDN.equals(dn)) {
                            // deserialize hex encoded object
                            final Serializable userDetailsObj = WebUtils.deserializeHexToObject(hexEncodedUserDetails);

                            // if we have a valid object, set the authentication token and bypass the remaining authentication processing chain
                            if (userDetailsObj instanceof NiFiUserDetails) {
                                final NiFiUserDetails userDetails = (NiFiUserDetails) userDetailsObj;
                                final NiFiUser user = userDetails.getNiFiUser();

                                // log the request attempt - response details will be logged later
                                logger.info(String.format("Attempting request for (%s) %s %s (source ip: %s)", user.getIdentity(), httpServletRequest.getMethod(),
                                        httpServletRequest.getRequestURL().toString(), request.getRemoteAddr()));

                                // we do not create the authentication token with the X509 certificate because the certificate is from the sending system, not the proxied user
                                final PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(userDetails, null, userDetails.getAuthorities());
                                token.setDetails(authenticationDetailsSource.buildDetails(request));
                                SecurityContextHolder.getContext().setAuthentication(token);
                            }
                        }
                    }
                } catch (final ClassNotFoundException cnfe) {
                    LOGGER.warn("Classpath issue detected because failed to deserialize authorized user in request header due to: " + cnfe, cnfe);
                }
            }
        }

        chain.doFilter(request, response);
    }
}
