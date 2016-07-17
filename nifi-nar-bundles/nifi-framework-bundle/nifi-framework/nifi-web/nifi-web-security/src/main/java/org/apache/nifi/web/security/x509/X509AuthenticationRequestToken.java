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
package org.apache.nifi.web.security.x509;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.NiFiAuthenticationRequestToken;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

import java.security.cert.X509Certificate;

/**
 * This is an authentication request with a given JWT token.
 */
public class X509AuthenticationRequestToken extends NiFiAuthenticationRequestToken {

    private final String proxiedEntitiesChain;
    private final X509PrincipalExtractor principalExtractor;
    private final X509Certificate[] certificates;

    /**
     * Creates a representation of the jwt authentication request for a user.
     *
     * @param proxiedEntitiesChain   The http servlet request
     * @param certificates  The certificate chain
     */
    public X509AuthenticationRequestToken(final String proxiedEntitiesChain, final X509PrincipalExtractor principalExtractor, final X509Certificate[] certificates, final String clientAddress) {
        super(clientAddress);
        setAuthenticated(false);
        this.proxiedEntitiesChain = proxiedEntitiesChain;
        this.principalExtractor = principalExtractor;
        this.certificates = certificates;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        if (StringUtils.isBlank(proxiedEntitiesChain)) {
            return principalExtractor.extractPrincipal(certificates[0]);
        } else {
            return String.format("%s<%s>", proxiedEntitiesChain, principalExtractor.extractPrincipal(certificates[0]));
        }
    }

    public String getProxiedEntitiesChain() {
        return proxiedEntitiesChain;
    }

    public X509Certificate[] getCertificates() {
        return certificates;
    }

    @Override
    public String toString() {
        return getName();
    }

}
