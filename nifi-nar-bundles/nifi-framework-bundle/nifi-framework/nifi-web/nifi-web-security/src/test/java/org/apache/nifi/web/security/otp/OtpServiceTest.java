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
package org.apache.nifi.web.security.otp;

import org.apache.nifi.web.security.token.OtpAuthenticationToken;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class OtpServiceTest {

    private final static String USER_1 = "user-identity-1";

    private OtpService otpService;

    @Before
    public void setUp() throws Exception {
        otpService = new OtpService();
    }

    @Test
    public void testGetAuthenticationForValidDownloadToken() throws Exception {
        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(USER_1);
        final String downloadToken = otpService.generateDownloadToken(authenticationToken);
        final String authenticatedUser = otpService.getAuthenticationFromDownloadToken(downloadToken);

        assertNotNull(authenticatedUser);
        assertEquals(USER_1, authenticatedUser);

        try {
            // ensure the token is no longer valid
            otpService.getAuthenticationFromDownloadToken(downloadToken);
            fail();
        } catch (final OtpAuthenticationException oae) {}
    }

    @Test
    public void testGetAuthenticationForValidUiExtensionToken() throws Exception {
        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(USER_1);
        final String uiExtensionToken = otpService.generateUiExtensionToken(authenticationToken);
        final String authenticatedUser = otpService.getAuthenticationFromUiExtensionToken(uiExtensionToken);

        assertNotNull(authenticatedUser);
        assertEquals(USER_1, authenticatedUser);

        try {
            // ensure the token is no longer valid
            otpService.getAuthenticationFromUiExtensionToken(uiExtensionToken);
            fail();
        } catch (final OtpAuthenticationException oae) {}
    }

    @Test(expected = OtpAuthenticationException.class)
    public void testGetNonExistentDownloadToken() throws Exception {
        otpService.getAuthenticationFromDownloadToken("Not a real download token");
    }

    @Test(expected = OtpAuthenticationException.class)
    public void testGetNonExistentUiExtensionToken() throws Exception {
        otpService.getAuthenticationFromUiExtensionToken("Not a real ui extension token");
    }

    @Test(expected = IllegalStateException.class)
    public void testMaxDownloadTokenLimit() throws Exception {
        // ensure we'll try to loop past the limit
        for (int i = 1; i < OtpService.MAX_CACHE_SOFT_LIMIT + 10; i++) {
            try {
                final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken("user-identity-" + i);
                otpService.generateDownloadToken(authenticationToken);
            } catch (final IllegalStateException iae) {
                // ensure we failed when we've past the limit
                assertEquals(OtpService.MAX_CACHE_SOFT_LIMIT + 1, i);
                throw iae;
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testMaxUiExtensionTokenLimit() throws Exception {
        // ensure we'll try to loop past the limit
        for (int i = 1; i < OtpService.MAX_CACHE_SOFT_LIMIT + 10; i++) {
            try {
                final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken("user-identity-" + i);
                otpService.generateUiExtensionToken(authenticationToken);
            } catch (final IllegalStateException iae) {
                // ensure we failed when we've past the limit
                assertEquals(OtpService.MAX_CACHE_SOFT_LIMIT + 1, i);
                throw iae;
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullTimeUnits() throws Exception {
        new OtpService(0, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeExpiration() throws Exception {
        new OtpService(-1, TimeUnit.MINUTES);
    }

    @Test(expected = OtpAuthenticationException.class)
    public void testUiExtensionTokenExpiration() throws Exception {
        final OtpService otpServiceWithTightExpiration = new OtpService(2, TimeUnit.SECONDS);

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(USER_1);
        final String downloadToken = otpServiceWithTightExpiration.generateUiExtensionToken(authenticationToken);

        // sleep for 4 seconds which should sufficiently expire the valid token
        Thread.sleep(4 * 1000);

        // attempt to get the token now that its expired
        otpServiceWithTightExpiration.getAuthenticationFromUiExtensionToken(downloadToken);
    }

    @Test(expected = OtpAuthenticationException.class)
    public void testDownloadTokenExpiration() throws Exception {
        final OtpService otpServiceWithTightExpiration = new OtpService(2, TimeUnit.SECONDS);

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(USER_1);
        final String downloadToken = otpServiceWithTightExpiration.generateDownloadToken(authenticationToken);

        // sleep for 4 seconds which should sufficiently expire the valid token
        Thread.sleep(4 * 1000);

        // attempt to get the token now that its expired
        otpServiceWithTightExpiration.getAuthenticationFromDownloadToken(downloadToken);
    }
}
