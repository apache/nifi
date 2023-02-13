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
package org.apache.nifi.web.security.jwt.revocation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardJwtLogoutListenerTest {
    private static final String ID = UUID.randomUUID().toString();

    private static final Instant EXPIRES = Instant.now();

    private static final String TOKEN = "TOKEN";

    private static final String TYPE_FIELD = "typ";

    private static final String JWT_TYPE = "JWT";

    @Mock
    private JwtRevocationService jwtRevocationService;

    @Mock
    private JwtDecoder jwtDecoder;

    private Jwt jwt;

    private StandardJwtLogoutListener listener;

    @BeforeEach
    public void setListener() {
        listener = new StandardJwtLogoutListener(jwtDecoder, jwtRevocationService);
        jwt = Jwt.withTokenValue(TOKEN).header(TYPE_FIELD, JWT_TYPE).jti(ID).expiresAt(EXPIRES).build();
    }

    @Test
    public void testLogoutBearerTokenNullZeroInteractions() {
        listener.logout(null);
        verifyNoInteractions(jwtDecoder);
        verifyNoInteractions(jwtRevocationService);
    }

    @Test
    public void testLogoutBearerToken() {
        when(jwtDecoder.decode(eq(TOKEN))).thenReturn(jwt);

        listener.logout(TOKEN);

        verify(jwtRevocationService).setRevoked(eq(ID), eq(EXPIRES));
    }
}
