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
package org.apache.nifi.web.security.jwt.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardIssuerJwtDecoderTest {
    private static final String HEADER_PAYLOAD = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJuaWZpIiwiaXNzIjoiaHR0cHM6Ly9uaWZpLmFwYWNoZS5vcmcifQ";

    private static final String TOKEN_VALUE = String.format("%s.cqEFAyICNyF5kDbYtsSgA73auanainaO44_q1GEDXeQ", HEADER_PAYLOAD);

    private static final String ISSUER = "https://nifi.apache.org";

    private static final String LOCALHOST_ISSUER = "https://localhost";

    private static final String TYPE_FIELD = "typ";

    private static final String JWT_TYPE = "JWT";

    @Mock
    private JwtDecoder applicationJwtDecoder;

    @Mock
    private JwtDecoderFactory<ClientRegistration> jwtDecoderFactory;

    @Mock
    private ClientRegistrationRepository clientRegistrationRepository;

    @Mock
    private ClientRegistration clientRegistration;

    @Mock
    private ClientRegistration.ProviderDetails providerDetails;

    @Mock
    private JwtDecoder clientRegistrationDecoder;

    @Test
    void testClientRegistrationNotConfigured() {
        when(clientRegistrationRepository.findByRegistrationId(anyString())).thenReturn(null);
        final StandardIssuerJwtDecoder decoder = new StandardIssuerJwtDecoder(applicationJwtDecoder, jwtDecoderFactory, clientRegistrationRepository);

        final Jwt jwt = getJwt();
        when(applicationJwtDecoder.decode(eq(TOKEN_VALUE))).thenReturn(jwt);

        final Jwt decoded = decoder.decode(TOKEN_VALUE);

        assertEquals(jwt, decoded);
    }

    @Test
    void testClientRegistrationConfiguredIssuerFound() {
        setClientRegistration();
        final StandardIssuerJwtDecoder decoder = new StandardIssuerJwtDecoder(applicationJwtDecoder, jwtDecoderFactory, clientRegistrationRepository);

        when(clientRegistration.getProviderDetails()).thenReturn(providerDetails);
        when(providerDetails.getIssuerUri()).thenReturn(ISSUER);
        final Jwt jwt = getJwt();
        when(clientRegistrationDecoder.decode(eq(TOKEN_VALUE))).thenReturn(jwt);

        final Jwt decoded = decoder.decode(TOKEN_VALUE);

        assertEquals(jwt, decoded);
    }

    @Test
    void testClientRegistrationConfiguredIssuerNotFound() {
        setClientRegistration();
        final StandardIssuerJwtDecoder decoder = new StandardIssuerJwtDecoder(applicationJwtDecoder, jwtDecoderFactory, clientRegistrationRepository);

        when(clientRegistration.getProviderDetails()).thenReturn(providerDetails);
        when(providerDetails.getIssuerUri()).thenReturn(LOCALHOST_ISSUER);
        final Jwt jwt = getJwt();
        when(applicationJwtDecoder.decode(eq(TOKEN_VALUE))).thenReturn(jwt);

        final Jwt decoded = decoder.decode(TOKEN_VALUE);

        assertEquals(jwt, decoded);
    }

    @Test
    void testClientRegistrationConfiguredTokenNotFound() {
        setClientRegistration();
        final StandardIssuerJwtDecoder decoder = new StandardIssuerJwtDecoder(applicationJwtDecoder, jwtDecoderFactory, clientRegistrationRepository);

        assertThrows(BadJwtException.class, () -> decoder.decode(null));
    }

    @Test
    void testClientRegistrationConfiguredTokenNotValid() {
        setClientRegistration();
        final StandardIssuerJwtDecoder decoder = new StandardIssuerJwtDecoder(applicationJwtDecoder, jwtDecoderFactory, clientRegistrationRepository);

        assertThrows(BadJwtException.class, () -> decoder.decode(String.class.getSimpleName()));
    }

    private void setClientRegistration() {
        when(clientRegistrationRepository.findByRegistrationId(anyString())).thenReturn(clientRegistration);
        when(jwtDecoderFactory.createDecoder(eq(clientRegistration))).thenReturn(clientRegistrationDecoder);
    }

    private Jwt getJwt() {
        return Jwt.withTokenValue(TOKEN_VALUE)
                .header(TYPE_FIELD, JWT_TYPE)
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plus(Duration.ofHours(1)))
                .build();
    }
}
