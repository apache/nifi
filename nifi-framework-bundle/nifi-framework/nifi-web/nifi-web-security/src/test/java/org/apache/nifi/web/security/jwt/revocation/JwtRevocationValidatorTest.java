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
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JwtRevocationValidatorTest {
    private static final String ID = UUID.randomUUID().toString();

    private static final String TOKEN = "TOKEN";

    private static final String TYPE_FIELD = "typ";

    private static final String JWT_TYPE = "JWT";

    @Mock
    private JwtRevocationService jwtRevocationService;

    private Jwt jwt;

    private JwtRevocationValidator validator;

    @BeforeEach
    public void setValidator() {
        validator = new JwtRevocationValidator(jwtRevocationService);
        jwt = Jwt.withTokenValue(TOKEN).header(TYPE_FIELD, JWT_TYPE).jti(ID).build();
    }

    @Test
    public void testValidateSuccess() {
        when(jwtRevocationService.isRevoked(eq(ID))).thenReturn(false);
        final OAuth2TokenValidatorResult result = validator.validate(jwt);
        assertFalse(result.hasErrors());
    }

    @Test
    public void testValidateFailure() {
        when(jwtRevocationService.isRevoked(eq(ID))).thenReturn(true);
        final OAuth2TokenValidatorResult result = validator.validate(jwt);
        assertTrue(result.hasErrors());
    }
}
