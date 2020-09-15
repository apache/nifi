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

import org.springframework.security.saml.SAMLCredential;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.Set;

public interface SAMLService {

    String SAML_SUPPORT_IS_NOT_CONFIGURED = "SAML support is not configured";

    /**
     * Initializes the service.
     */
    void initialize();

    /**
     * @return whether SAML support is enabled
     */
    boolean isSamlEnabled();

    /**
     * @return true if the service provider metadata has been initialized, false otherwise
     */
    boolean isServiceProviderInitialized();

    /**
     * Initializes the service provider metadata.
     *
     * This method must be called before using the service to perform any other SAML operations.
     *
     * @param baseUrl the baseUrl of the service provider
     */
    void initializeServiceProvider(String baseUrl);

    /**
     * Retrieves the service provider metadata XML.
     */
    String getServiceProviderMetadata();

    /**
     * Retrieves the expiration time in milliseconds for a SAML authentication.
     *
     * @return the authentication
     */
    long getAuthExpiration();

    /**
     * Initiates a login sequence with the SAML identity provider.
     *
     * @param request servlet request
     * @param response servlet response
     */
    void initiateLogin(HttpServletRequest request, HttpServletResponse response, String relayState);

    /**
     * Processes the assertions coming back from the identity provider and returns a NiFi JWT.
     *
     * @param request servlet request
     * @param response servlet request
     * @param parameters a map of parameters
     * @return a NiFi JWT
     */
    SAMLCredential processLogin(HttpServletRequest request, HttpServletResponse response, Map<String,String> parameters);

    /**
     * Returns the identity of the user based on the given credential.
     *
     * If no identity attribute is specified in nifi.properties, then the NameID of the Subject will be used.
     *
     * Otherwise the value of the given identity attribute will be used.
     *
     * @param samlCredential the SAML credential returned from a successful authentication
     * @return the user identity
     */
    String getUserIdentity(SAMLCredential samlCredential);

    /**
     * Returns the names of the groups the user belongs from looking at the assertions in the credential.
     *
     * Requires configuring the name of the group attribute in nifi.properties, otherwise an empty set will be returned.
     *
     * @param credential the SAML credential returned from a successful authentication
     * @return the set of groups the user belongs to, or empty set if none exist or if nifi has not been configured with a group attribute name
     */
    Set<String> getUserGroups(SAMLCredential credential);

    /**
     * Initiates a logout sequence with the SAML identity provider.
     *
     * @param request servlet request
     * @param response servlet response
     */
    void initiateLogout(HttpServletRequest request, HttpServletResponse response, SAMLCredential credential);

    /**
     * Processes a logout, typically a response from previously initiating a logout, but may be an IDP initiated logout.
     *
     * @param request servlet request
     * @param response servlet response
     * @param parameters a map of parameters
     */
    void processLogout(HttpServletRequest request, HttpServletResponse response, Map<String,String> parameters);

    /**
     * Shuts down the service.
     */
    void shutdown();

}
