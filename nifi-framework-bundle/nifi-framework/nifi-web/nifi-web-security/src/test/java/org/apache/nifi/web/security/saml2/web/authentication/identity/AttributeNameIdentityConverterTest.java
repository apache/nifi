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

import org.junit.jupiter.api.Test;
import org.springframework.security.saml2.provider.service.authentication.Saml2ResponseAssertion;
import org.springframework.security.saml2.provider.service.authentication.Saml2ResponseAssertionAccessor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttributeNameIdentityConverterTest {
    private static final String ATTRIBUTE_NAME = "urn:oid:0.9.2342.19200300.100.1.3";

    private static final String ATTRIBUTE_VALUE = "user@localhost.localdomain";

    private static final String OTHER_ATTRIBUTE_NAME = "urn:oid:2.5.4.3";

    private static final String NAME_IDENTIFIER = "name-identifier";

    private static final String RESPONSE_VALUE = "<saml2p:Response/>";

    private final AttributeNameIdentityConverter converter = new AttributeNameIdentityConverter(ATTRIBUTE_NAME);

    @Test
    void testConvertConfiguredAttributeFound() {
        final Saml2ResponseAssertionAccessor assertion = getAssertion(Map.of(ATTRIBUTE_NAME, List.of(ATTRIBUTE_VALUE)));

        assertEquals(ATTRIBUTE_VALUE, converter.convert(assertion));
    }

    @Test
    void testConvertConfiguredAttributeNotFoundReturnsNameIdentifier() {
        final Saml2ResponseAssertionAccessor assertionWithoutAttributes = getAssertion(Map.of());

        assertEquals(NAME_IDENTIFIER, converter.convert(assertionWithoutAttributes));

        final Saml2ResponseAssertionAccessor assertionWithOtherAttribute = getAssertion(Map.of(OTHER_ATTRIBUTE_NAME, List.of(ATTRIBUTE_VALUE)));

        assertEquals(NAME_IDENTIFIER, converter.convert(assertionWithOtherAttribute));
    }

    private Saml2ResponseAssertionAccessor getAssertion(final Map<String, List<Object>> attributes) {
        return Saml2ResponseAssertion.withResponseValue(RESPONSE_VALUE)
                .nameId(NAME_IDENTIFIER)
                .attributes(attributes)
                .build();
    }
}
