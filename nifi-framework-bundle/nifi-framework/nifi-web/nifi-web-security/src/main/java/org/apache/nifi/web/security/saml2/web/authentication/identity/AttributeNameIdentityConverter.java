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
package org.apache.nifi.web.security.saml2.web.authentication.identity;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.saml2.provider.service.authentication.Saml2AuthenticatedPrincipal;

import java.util.Objects;

/**
 * Converter for customized User Identity using SAML Attribute Value
 */
public class AttributeNameIdentityConverter implements Converter<Saml2AuthenticatedPrincipal, String> {
    private final String attributeName;

    public AttributeNameIdentityConverter(final String attributeName) {
        this.attributeName = Objects.requireNonNull(attributeName, "Attribute Name required");
    }

    /**
     * Convert Principal to identity using configured attribute name when found
     *
     * @param principal SAML 2 Authenticated Principal
     * @return Attribute Value or Principal Name when attribute not found
     */
    @Override
    public String convert(final Saml2AuthenticatedPrincipal principal) {
        final Object attribute = principal.getFirstAttribute(attributeName);
        return attribute == null ? principal.getName() : attribute.toString();
    }
}
