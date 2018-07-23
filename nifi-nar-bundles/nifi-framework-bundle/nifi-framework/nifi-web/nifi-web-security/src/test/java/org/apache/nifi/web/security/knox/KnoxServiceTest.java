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
package org.apache.nifi.web.security.knox;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.oauth2.sdk.auth.JWTAuthenticationClaimsSet;
import com.nimbusds.oauth2.sdk.auth.PrivateKeyJWT;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.JWTID;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KnoxServiceTest {

    private static final String AUDIENCE = "https://apache-knox/token";
    private static final String AUDIENCE_2 = "https://apache-knox-2/token";

    @Test(expected = IllegalStateException.class)
    public void testKnoxSsoNotEnabledGetKnoxUrl() throws Exception {
        final KnoxConfiguration configuration = mock(KnoxConfiguration.class);
        when(configuration.isKnoxEnabled()).thenReturn(false);

        final KnoxService service = new KnoxService(configuration);
        assertFalse(service.isKnoxEnabled());

        service.getKnoxUrl();
    }

    @Test(expected = IllegalStateException.class)
    public void testKnoxSsoNotEnabledGetAuthenticatedFromToken() throws Exception {
        final KnoxConfiguration configuration = mock(KnoxConfiguration.class);
        when(configuration.isKnoxEnabled()).thenReturn(false);

        final KnoxService service = new KnoxService(configuration);
        assertFalse(service.isKnoxEnabled());

        service.getAuthenticationFromToken("jwt-token-value");
    }

    private JWTAuthenticationClaimsSet getAuthenticationClaimsSet(final String subject, final String audience, final Date expiration) {
        return new JWTAuthenticationClaimsSet(
                new ClientID(subject),
                new Audience(audience).toSingleAudienceList(),
                expiration,
                null,
                null,
                new JWTID());
    }

    @Test
    public void testSignedJwt() throws Exception {
        final String subject = "user-1";
        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        final KeyPair pair = keyGen.generateKeyPair();
        final RSAPrivateKey privateKey = (RSAPrivateKey) pair.getPrivate();
        final RSAPublicKey publicKey = (RSAPublicKey) pair.getPublic();

        final JWTAuthenticationClaimsSet claimsSet = getAuthenticationClaimsSet(subject, AUDIENCE, expiration);
        final PrivateKeyJWT privateKeyJWT = new PrivateKeyJWT(claimsSet, JWSAlgorithm.RS256, privateKey, null, null);

        final KnoxConfiguration configuration = getConfiguration(publicKey);
        final KnoxService service = new KnoxService(configuration);

        Assert.assertEquals(subject, service.getAuthenticationFromToken(privateKeyJWT.getClientAssertion().serialize()));
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testBadSignedJwt() throws Exception {
        final String subject = "user-1";
        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");

        final KeyPair pair1 = keyGen.generateKeyPair();
        final RSAPrivateKey privateKey1 = (RSAPrivateKey) pair1.getPrivate();

        final KeyPair pair2 = keyGen.generateKeyPair();
        final RSAPublicKey publicKey2 = (RSAPublicKey) pair2.getPublic();

        // sign the jwt with pair 1
        final JWTAuthenticationClaimsSet claimsSet = getAuthenticationClaimsSet(subject, AUDIENCE, expiration);
        final PrivateKeyJWT privateKeyJWT = new PrivateKeyJWT(claimsSet, JWSAlgorithm.RS256, privateKey1, null, null);

        // attempt to verify it with pair 2
        final KnoxConfiguration configuration = getConfiguration(publicKey2);
        final KnoxService service = new KnoxService(configuration);

        service.getAuthenticationFromToken(privateKeyJWT.getClientAssertion().serialize());
    }

    @Test(expected = ParseException.class)
    public void testPlainJwt() throws Exception {
        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        final KeyPair pair = keyGen.generateKeyPair();
        final RSAPublicKey publicKey = (RSAPublicKey) pair.getPublic();

        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));
        final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .subject("user-1")
                .expirationTime(expiration)
                .build();

        final PlainJWT plainJWT = new PlainJWT(claimsSet);

        final KnoxConfiguration configuration = getConfiguration(publicKey);
        final KnoxService service = new KnoxService(configuration);

        service.getAuthenticationFromToken(plainJWT.serialize());
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testExpiredJwt() throws Exception {
        final String subject = "user-1";

        // token expires in 1 sec
        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));

        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        final KeyPair pair = keyGen.generateKeyPair();
        final RSAPrivateKey privateKey = (RSAPrivateKey) pair.getPrivate();
        final RSAPublicKey publicKey = (RSAPublicKey) pair.getPublic();

        // wait 2 sec
        Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS));

        final JWTAuthenticationClaimsSet claimsSet = getAuthenticationClaimsSet(subject, AUDIENCE, expiration);
        final PrivateKeyJWT privateKeyJWT = new PrivateKeyJWT(claimsSet, JWSAlgorithm.RS256, privateKey, null, null);

        final KnoxConfiguration configuration = getConfiguration(publicKey);
        final KnoxService service = new KnoxService(configuration);

        service.getAuthenticationFromToken(privateKeyJWT.getClientAssertion().serialize());
    }

    @Test
    public void testRequiredAudience() throws Exception {
        final String subject = "user-1";
        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        final KeyPair pair = keyGen.generateKeyPair();
        final RSAPrivateKey privateKey = (RSAPrivateKey) pair.getPrivate();
        final RSAPublicKey publicKey = (RSAPublicKey) pair.getPublic();

        final JWTAuthenticationClaimsSet claimsSet = getAuthenticationClaimsSet(subject, AUDIENCE, expiration);
        final PrivateKeyJWT privateKeyJWT = new PrivateKeyJWT(claimsSet, JWSAlgorithm.RS256, privateKey, null, null);

        final KnoxConfiguration configuration = getConfiguration(publicKey);
        when(configuration.getAudiences()).thenReturn(null);
        final KnoxService service = new KnoxService(configuration);

        Assert.assertEquals(subject, service.getAuthenticationFromToken(privateKeyJWT.getClientAssertion().serialize()));
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testInvalidAudience() throws Exception {
        final String subject = "user-1";
        final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        final KeyPair pair = keyGen.generateKeyPair();
        final RSAPrivateKey privateKey = (RSAPrivateKey) pair.getPrivate();
        final RSAPublicKey publicKey = (RSAPublicKey) pair.getPublic();

        final JWTAuthenticationClaimsSet claimsSet = getAuthenticationClaimsSet(subject, "incorrect-audience", expiration);
        final PrivateKeyJWT privateKeyJWT = new PrivateKeyJWT(claimsSet, JWSAlgorithm.RS256, privateKey, null, null);

        final KnoxConfiguration configuration = getConfiguration(publicKey);
        final KnoxService service = new KnoxService(configuration);

        Assert.assertEquals(subject, service.getAuthenticationFromToken(privateKeyJWT.getClientAssertion().serialize()));
    }

    private KnoxConfiguration getConfiguration(final RSAPublicKey publicKey) throws Exception {
        final KnoxConfiguration configuration = mock(KnoxConfiguration.class);
        when(configuration.isKnoxEnabled()).thenReturn(true);
        when(configuration.getKnoxUrl()).thenReturn("knox-sso-url");
        when(configuration.getKnoxCookieName()).thenReturn("knox-cookie-name");
        when(configuration.getAudiences()).thenReturn(Stream.of(AUDIENCE, AUDIENCE_2).collect(Collectors.toSet()));
        when(configuration.getKnoxPublicKey()).thenReturn(publicKey);
        return configuration;
    }
}