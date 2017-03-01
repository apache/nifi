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

package org.apache.nifi.minifi.c2.security.authentication;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;

public class X509AuthenticationToken extends AbstractAuthenticationToken {
    private final X509Certificate[] x509Certificates;
    private final String subjectDn;

    public X509AuthenticationToken(X509Certificate[] x509Certificates) {
        this(x509Certificates, null);
        setAuthenticated(false);
    }

    protected X509AuthenticationToken(X509Certificate[] x509Certificates, Collection<GrantedAuthority> grantedAuthorities) {
        super(grantedAuthorities);
        this.x509Certificates = Arrays.copyOf(x509Certificates, x509Certificates.length, X509Certificate[].class);
        X509Certificate x509Certificate = x509Certificates[0];
        this.subjectDn = x509Certificate.getSubjectDN().getName().trim();
    }

    @Override
    public X509Certificate[] getCredentials() {
        return x509Certificates;
    }

    @Override
    public String getPrincipal() {
        return subjectDn;
    }
}
