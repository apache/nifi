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
package org.apache.nifi.web.security.token;

import org.apache.nifi.security.util.CertificateUtils;
import org.springframework.security.authentication.AbstractAuthenticationToken;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This is an Authentication Token for logging in. Once a user is authenticated, they can be issued an ID token.
 */
public class LoginAuthenticationToken extends AbstractAuthenticationToken {

    private final String identity;
    private final String username;
    private final long expiration;
    private final String issuer;

    /**
     * Creates a representation of the authentication token for a user.
     *
     * @param identity   The unique identifier for this user
     * @param expiration The relative time to expiration in milliseconds
     * @param issuer     The IdentityProvider implementation that generated this token
     */
    public LoginAuthenticationToken(final String identity, final long expiration, final String issuer) {
        this(identity, null, expiration, issuer);
    }

    /**
     * Creates a representation of the authentication token for a user.
     *
     * @param identity   The unique identifier for this user (cannot be null or empty)
     * @param username   The preferred username for this user
     * @param expiration The relative time to expiration in milliseconds
     * @param issuer     The IdentityProvider implementation that generated this token
     */
    public LoginAuthenticationToken(final String identity, final String username, final long expiration, final String issuer) {
        super(null);
        setAuthenticated(true);
        this.identity = identity;
        this.username = username;
        this.issuer = issuer;
        Calendar now = Calendar.getInstance();
        this.expiration = now.getTimeInMillis() + expiration;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return identity;
    }

    /**
     * Returns the expiration instant in milliseconds. This value is an absolute point in time (i.e. Nov
     * 16, 2015 11:30:00.000 GMT), not a relative time (i.e. 60 minutes). It is calculated by adding the
     * relative expiration from the constructor to the timestamp at object creation.
     *
     * @return the expiration in millis
     */
    public long getExpiration() {
        return expiration;
    }

    public String getIssuer() {
        return issuer;
    }

    @Override
    public String getName() {
        if (username == null) {
            // if the username is a DN this will extract the username or CN... if not will return what was passed
            return CertificateUtils.extractUsername(identity);
        } else {
            return username;
        }
    }

    @Override
    public String toString() {
        Calendar expirationTime = Calendar.getInstance();
        expirationTime.setTimeInMillis(getExpiration());
        long remainingTime = expirationTime.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
        dateFormat.setTimeZone(expirationTime.getTimeZone());
        String expirationTimeString = dateFormat.format(expirationTime.getTime());

        return new StringBuilder("LoginAuthenticationToken for ")
                .append(getName())
                .append(" issued by ")
                .append(getIssuer())
                .append(" expiring at ")
                .append(expirationTimeString)
                .append(" [")
                .append(getExpiration())
                .append(" ms, ")
                .append(remainingTime)
                .append(" ms remaining]")
                .toString();
    }

}
