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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardBearerTokenProviderTest {
    private static final String IDENTITY = "IDENTITY";

    private static final Instant EXPIRATION = Instant.now().plusSeconds(300);

    private static final Duration MAXIMUM_DURATION_EXCEEDED = Duration.parse("PT12H5M");

    private static final Duration MINIMUM_DURATION_EXCEEDED = Duration.parse("PT30S");

    private static final URI ISSUER = URI.create("https://localhost:8443");

    private static final String KEY_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 4096;

    private static final JWSAlgorithm JWS_ALGORITHM = JWSAlgorithm.PS512;

    private static final String GROUP = "ProviderGroup";

    private static KeyPair keyPair;

    @Mock
    private JwsSignerProvider jwsSignerProvider;

    @Mock
    private IssuerProvider issuerProvider;

    private StandardBearerTokenProvider provider;

    private JWSVerifier jwsVerifier;

    @BeforeAll
    public static void setKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPairGenerator.initialize(KEY_SIZE);
        keyPair = keyPairGenerator.generateKeyPair();
    }

    @BeforeEach
    public void setProvider() {
        provider = new StandardBearerTokenProvider(jwsSignerProvider, issuerProvider);

        jwsVerifier = new RSASSAVerifier((RSAPublicKey) keyPair.getPublic());
        final JWSSigner jwsSigner = new RSASSASigner(keyPair.getPrivate());

        final String keyIdentifier = UUID.randomUUID().toString();
        final JwsSignerContainer jwsSignerContainer = new JwsSignerContainer(keyIdentifier, JWS_ALGORITHM, jwsSigner);
        when(jwsSignerProvider.getJwsSignerContainer(isA(Instant.class))).thenReturn(jwsSignerContainer);

        when(issuerProvider.getIssuer()).thenReturn(ISSUER);
    }

    @Test
    public void testGetBearerToken() throws ParseException, JOSEException {
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(IDENTITY, EXPIRATION, Collections.emptySet());

        final String bearerToken = provider.getBearerToken(loginAuthenticationToken);

        assertTokenMatched(bearerToken, loginAuthenticationToken);
    }

    @Test
    public void testGetBearerTokenGroups() throws ParseException, JOSEException {
        final GrantedAuthority grantedAuthority = new SimpleGrantedAuthority(GROUP);
        final Collection<GrantedAuthority> authorities = Collections.singletonList(grantedAuthority);

        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(IDENTITY, EXPIRATION, authorities);

        final String bearerToken = provider.getBearerToken(loginAuthenticationToken);

        assertTokenMatched(bearerToken, loginAuthenticationToken);
    }

    @Test
    public void testGetBearerTokenExpirationMaximum() throws ParseException, JOSEException {
        final Instant expiration = Instant.now().plus(MAXIMUM_DURATION_EXCEEDED);
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(IDENTITY, expiration, Collections.emptySet());

        final String bearerToken = provider.getBearerToken(loginAuthenticationToken);

        final SignedJWT signedJwt = assertTokenVerified(bearerToken);
        final JWTClaimsSet claims = signedJwt.getJWTClaimsSet();
        final Date claimExpirationTime = claims.getExpirationTime();
        assertNotNull(claimExpirationTime, "Expiration Time not found");

        final Date loginExpirationTime = Date.from(loginAuthenticationToken.getExpiration());
        assertNotSame(loginExpirationTime.toString(), claimExpirationTime.toString(), "Expiration Time matched");

        assertTrue(claimExpirationTime.toInstant().isBefore(loginExpirationTime.toInstant()), "Claim Expiration after Login Expiration");
    }

    @Test
    public void testGetBearerTokenExpirationMinimum() throws ParseException, JOSEException {
        final Instant expiration = Instant.now().plus(MINIMUM_DURATION_EXCEEDED);
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(IDENTITY, expiration, Collections.emptySet());

        final String bearerToken = provider.getBearerToken(loginAuthenticationToken);

        final SignedJWT signedJwt = assertTokenVerified(bearerToken);
        final JWTClaimsSet claims = signedJwt.getJWTClaimsSet();
        final Date claimExpirationTime = claims.getExpirationTime();
        assertNotNull(claimExpirationTime, "Expiration Time not found");

        final Date loginExpirationTime = Date.from(loginAuthenticationToken.getExpiration());
        assertNotSame(loginExpirationTime.toString(), claimExpirationTime.toString(), "Expiration Time matched");

        assertTrue(claimExpirationTime.toInstant().isAfter(loginExpirationTime.toInstant()), "Claim Expiration before Login Expiration");
    }

    private SignedJWT assertTokenVerified(final String bearerToken) throws ParseException, JOSEException {
        final SignedJWT signedJwt = SignedJWT.parse(bearerToken);
        assertTrue(signedJwt.verify(jwsVerifier), "Verification Failed");
        return signedJwt;
    }

    private void assertTokenMatched(final String bearerToken, final LoginAuthenticationToken loginAuthenticationToken) throws ParseException, JOSEException {
        final SignedJWT signedJwt = assertTokenVerified(bearerToken);
        final JWTClaimsSet claims = signedJwt.getJWTClaimsSet();
        assertNotNull(claims.getIssueTime(), "Issue Time not found");
        assertNotNull(claims.getNotBeforeTime(), "Not Before Time not found");

        final Date claimExpirationTime = claims.getExpirationTime();
        assertNotNull(claimExpirationTime, "Expiration Time not found");

        final Date loginExpirationTime = Date.from(loginAuthenticationToken.getExpiration());
        assertEquals(loginExpirationTime.toString(), claimExpirationTime.toString(), "Expiration Time not matched");

        assertEquals(ISSUER.toString(), claims.getIssuer());
        assertEquals(Collections.singletonList(ISSUER.toString()), claims.getAudience());
        assertEquals(IDENTITY, claims.getSubject());
        assertEquals(IDENTITY, claims.getClaim(SupportedClaim.PREFERRED_USERNAME.getClaim()));
        assertNotNull(claims.getJWTID(), "JSON Web Token Identifier not found");

        final List<String> groups = claims.getStringListClaim(SupportedClaim.GROUPS.getClaim());
        assertNotNull(groups);

        final Collection<GrantedAuthority> grantedAuthorities = loginAuthenticationToken.getAuthorities();
        final List<String> authorities = grantedAuthorities.stream().map(GrantedAuthority::getAuthority).collect(Collectors.toList());

        assertEquals(authorities, groups);
    }
}
