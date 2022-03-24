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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * KnoxService is a service for managing the Apache Knox SSO.
 */
public class KnoxService {

    private static final Logger logger = LoggerFactory.getLogger(KnoxService.class);

    private KnoxConfiguration configuration;
    private JWSVerifier verifier;
    private String knoxUrl;
    private Set<String> audiences;

    /**
     * Creates a new KnoxService.
     *
     * @param configuration          knox configuration
     */
    public KnoxService(final KnoxConfiguration configuration) {
        this.configuration = configuration;

        // if knox sso support is enabled, validate the configuration
        if (configuration.isKnoxEnabled()) {
            // ensure the url is provided
            knoxUrl = configuration.getKnoxUrl();
            if (StringUtils.isBlank(knoxUrl)) {
                throw new RuntimeException("Knox URL is required when Apache Knox SSO support is enabled.");
            }

            // ensure the cookie name is set
            if (StringUtils.isBlank(configuration.getKnoxCookieName())) {
                throw new RuntimeException("Knox Cookie Name is required when Apache Knox SSO support is enabled.");
            }

            // create the verifier
            verifier = new RSASSAVerifier(configuration.getKnoxPublicKey());

            // get the audience
            audiences = configuration.getAudiences();
        }
    }

    /**
     * Returns whether Knox support is enabled.
     *
     * @return whether Knox support is enabled
     */
    public boolean isKnoxEnabled() {
        return configuration.isKnoxEnabled();
    }

    /**
     * Returns the Knox Url.
     *
     * @return knox url
     */
    public String getKnoxUrl() {
        if (!configuration.isKnoxEnabled()) {
            throw new IllegalStateException("Apache Knox SSO is not enabled.");
        }

        return knoxUrl;
    }

    /**
     * Extracts the authentication from the token and verify it.
     *
     * @param jwt signed jwt string
     * @return the user authentication
     * @throws ParseException if the payload of the jwt doesn't represent a valid json object and a jwt claims set
     * @throws JOSEException if the JWS object couldn't be verified
     */
    public String getAuthenticationFromToken(final String jwt) throws ParseException, JOSEException {
        if (!configuration.isKnoxEnabled()) {
            throw new IllegalStateException("Apache Knox SSO is not enabled.");
        }

        // attempt to parse the signed jwt
        final SignedJWT signedJwt = SignedJWT.parse(jwt);

        // validate the token
        if (validateToken(signedJwt)) {
            final JWTClaimsSet claimsSet = signedJwt.getJWTClaimsSet();
            if (claimsSet == null) {
                logger.info("Claims set is missing from Knox JWT.");
                throw new InvalidAuthenticationException("The Knox JWT token is not valid.");
            }

            // extract the user identity from the token
            return claimsSet.getSubject();
        } else {
            throw new InvalidAuthenticationException("The Knox JWT token is not valid.");
        }
    }

    /**
     * Validate the specified jwt.
     *
     * @param jwtToken knox jwt
     * @return whether this jwt is valid
     * @throws JOSEException if the jws object couldn't be verified
     * @throws ParseException if the payload of the jwt doesn't represent a valid json object and a jwt claims set
     */
    private boolean validateToken(final SignedJWT jwtToken) throws JOSEException, ParseException {
        final boolean validSignature = validateSignature(jwtToken);
        final boolean validAudience = validateAudience(jwtToken);
        final boolean notExpired = validateExpiration(jwtToken);

        return validSignature && validAudience && notExpired;
    }

    /**
     * Validate the jwt signature.
     *
     * @param jwtToken knox jwt
     * @return whether this jwt signature is valid
     * @throws JOSEException if the jws object couldn't be verified
     */
    private boolean validateSignature(final SignedJWT jwtToken) throws JOSEException {
        boolean valid = false;

        // ensure the token is signed
        if (JWSObject.State.SIGNED.equals(jwtToken.getState())) {

            // ensure the signature is present
            if (jwtToken.getSignature() != null) {

                // verify the token
                valid = jwtToken.verify(verifier);
            }
        }

        if (!valid) {
            logger.error("The Knox JWT has an invalid signature.");
        }

        return valid;
    }

    /**
     * Validate the jwt audience.
     *
     * @param jwtToken knox jwt
     * @return whether this jwt audience is valid
     * @throws ParseException if the payload of the jwt doesn't represent a valid json object and a jwt claims set
     */
    private boolean validateAudience(final SignedJWT jwtToken) throws ParseException {
        if (audiences == null) {
            return true;
        }

        final JWTClaimsSet claimsSet = jwtToken.getJWTClaimsSet();
        if (claimsSet == null) {
            logger.error("Claims set is missing from Knox JWT.");
            return false;
        }

        final List<String> tokenAudiences = claimsSet.getAudience();
        if (tokenAudiences == null) {
            logger.error("Audience is missing from the Knox JWT.");
            return false;
        }

        boolean valid = false;
        for (final String tokenAudience : tokenAudiences) {
            // ensure one of the audiences is matched
            if (audiences.contains(tokenAudience)) {
                valid = true;
                break;
            }
        }

        if (!valid) {
            logger.error(String.format("The Knox JWT does not have the required audience(s). Required one of [%s]. Present in JWT [%s].",
                    StringUtils.join(audiences, ", "), StringUtils.join(tokenAudiences, ", ")));
        }

        return valid;
    }

    /**
     * Validate the jwt expiration.
     *
     * @param jwtToken knox jwt
     * @return whether this jwt is not expired
     * @throws ParseException if the payload of the jwt doesn't represent a valid json object and a jwt claims set
     */
    private boolean validateExpiration(final SignedJWT jwtToken) throws ParseException {
        boolean valid = false;

        final JWTClaimsSet claimsSet = jwtToken.getJWTClaimsSet();
        if (claimsSet == null) {
            logger.error("Claims set is missing from Knox JWT.");
            return false;
        }

        final Date now = new Date();
        final Date expiration = claimsSet.getExpirationTime();

        // the token is not expired if the expiration isn't present or the expiration is after now
        if (expiration == null || now.before(expiration)) {
            valid = true;
        }

        if (!valid) {
            logger.error("The Knox JWT is expired.");
        }

        return valid;
    }
}
