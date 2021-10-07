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
package org.apache.nifi.web.security.jwt.provider;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.nifi.web.security.jwt.jws.JwsSignerContainer;
import org.apache.nifi.web.security.jwt.jws.JwsSignerProvider;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StandardBearerTokenProviderTest {
    private static final String USERNAME = "USERNAME";

    private static final String IDENTITY = "IDENTITY";

    private static final long EXPIRATION = 60;

    private static final String ISSUER = "ISSUER";

    private static final String KEY_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 4096;

    private static final JWSAlgorithm JWS_ALGORITHM = JWSAlgorithm.PS512;

    @Mock
    private JwsSignerProvider jwsSignerProvider;

    private StandardBearerTokenProvider provider;

    private JWSVerifier jwsVerifier;

    private JWSSigner jwsSigner;

    @Before
    public void setProvider() throws NoSuchAlgorithmException {
        provider = new StandardBearerTokenProvider(jwsSignerProvider);

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPairGenerator.initialize(KEY_SIZE);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        jwsVerifier = new RSASSAVerifier((RSAPublicKey) keyPair.getPublic());
        jwsSigner = new RSASSASigner(keyPair.getPrivate());
    }

    @Test
    public void testGetBearerToken() throws ParseException, JOSEException {
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(IDENTITY, USERNAME, EXPIRATION, ISSUER);
        final String keyIdentifier = UUID.randomUUID().toString();
        final JwsSignerContainer jwsSignerContainer = new JwsSignerContainer(keyIdentifier, JWS_ALGORITHM, jwsSigner);
        when(jwsSignerProvider.getJwsSignerContainer(isA(Instant.class))).thenReturn(jwsSignerContainer);

        final String bearerToken = provider.getBearerToken(loginAuthenticationToken);

        final SignedJWT signedJwt = SignedJWT.parse(bearerToken);
        assertTrue("Verification Failed", signedJwt.verify(jwsVerifier));

        final JWTClaimsSet claims = signedJwt.getJWTClaimsSet();
        assertNotNull("Issue Time not found", claims.getIssueTime());
        assertNotNull("Not Before Time Time not found", claims.getNotBeforeTime());
        assertNotNull("Expiration Time Time not found", claims.getExpirationTime());
        assertEquals(ISSUER, claims.getIssuer());
        assertEquals(Collections.singletonList(ISSUER), claims.getAudience());
        assertEquals(IDENTITY, claims.getSubject());
        assertEquals(USERNAME, claims.getClaim(SupportedClaim.PREFERRED_USERNAME.getClaim()));
        assertNotNull("JSON Web Token Identifier not found", claims.getJWTID());
    }
}
