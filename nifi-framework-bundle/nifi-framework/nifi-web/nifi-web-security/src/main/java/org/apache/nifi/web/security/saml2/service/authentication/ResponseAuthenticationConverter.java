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
package org.apache.nifi.web.security.saml2.service.authentication;

import org.apache.nifi.util.StringUtils;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.schema.XSAny;
import org.opensaml.core.xml.schema.XSString;
import org.opensaml.saml.saml2.core.Assertion;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.saml2.provider.service.authentication.OpenSaml5AuthenticationProvider;
import org.springframework.security.saml2.provider.service.authentication.OpenSaml5AuthenticationProvider.ResponseToken;
import org.springframework.security.saml2.provider.service.authentication.Saml2AuthenticatedPrincipal;
import org.springframework.security.saml2.provider.service.authentication.Saml2Authentication;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Converter from SAML 2 Response Token to SAML 2 Authentication for Spring Security
 */
public class ResponseAuthenticationConverter implements Converter<ResponseToken, Saml2Authentication> {
    private static final Converter<ResponseToken, Saml2Authentication> defaultConverter = OpenSaml5AuthenticationProvider.createDefaultResponseAuthenticationConverter();

    private final String groupAttributeName;

    /**
     * Response Authentication Converter with optional Group Attribute Name
     *
     * @param groupAttributeName Group Attribute Name is not required
     */
    public ResponseAuthenticationConverter(final String groupAttributeName) {
        this.groupAttributeName = groupAttributeName;
    }

    /**
     * Convert SAML 2 Response Token using default Converter and process authorities based on Group Attribute Name
     *
     * @param responseToken SAML 2 Response Token
     * @return SAML 2 Authentication
     */
    @Override
    public Saml2Authentication convert(final ResponseToken responseToken) {
        Objects.requireNonNull(responseToken, "Response Token required");
        final List<Assertion> assertions = responseToken.getResponse().getAssertions();
        final Saml2Authentication authentication = Objects.requireNonNull(defaultConverter.convert(responseToken), "Authentication required");
        final Saml2AuthenticatedPrincipal principal = (Saml2AuthenticatedPrincipal) authentication.getPrincipal();
        return new Saml2Authentication(principal, authentication.getSaml2Response(), getAuthorities(assertions));
    }

    private Collection<? extends GrantedAuthority> getAuthorities(final List<Assertion> assertions) {
        final Collection<? extends GrantedAuthority> authorities;

        if (StringUtils.isBlank(groupAttributeName)) {
            authorities = Collections.emptyList();
        } else {
            // Stream Assertions to Attributes and filter based on Group Attribute Name
            authorities = assertions.stream()
                    .flatMap(assertion -> assertion.getAttributeStatements().stream())
                    .flatMap(attributeStatement -> attributeStatement.getAttributes().stream())
                    .filter(attribute -> groupAttributeName.equals(attribute.getName()))
                    .flatMap(attribute -> attribute.getAttributeValues().stream())
                    .map(this::getAttributeValue)
                    .filter(Objects::nonNull)
                    .map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toList());
        }

        return authorities;
    }

    private String getAttributeValue(final XMLObject xmlObject) {
        final String attributeValue;

        if (xmlObject instanceof XSAny any) {
            attributeValue = any.getTextContent();
        } else if (xmlObject instanceof XSString string) {
            attributeValue = string.getValue();
        } else {
            attributeValue = null;
        }

        return attributeValue;
    }
}
