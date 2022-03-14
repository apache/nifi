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
package org.apache.nifi.oauth2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccessTokenTest {
    private static final String ACCESS_TOKEN = "ACCESS";

    private static final String REFRESH_TOKEN = "REFRESH";

    private static final String TOKEN_TYPE = "Bearer";

    private static final String SCOPES = "default";

    private static final long TWO_SECONDS_AGO = -2;

    private static final long TEN_SECONDS_AGO = -10;

    private static final long IN_SIXTY_SECONDS = 60;

    @Test
    public void testIsExpiredTenSecondsAgo() {
        final AccessToken accessToken = getAccessToken(TEN_SECONDS_AGO);

        assertTrue(accessToken.isExpired());
    }

    @Test
    public void testIsExpiredTwoSecondsAgo() {
        final AccessToken accessToken = getAccessToken(TWO_SECONDS_AGO);

        assertFalse(accessToken.isExpired());
    }

    @Test
    public void testIsExpiredInSixtySeconds() {
        final AccessToken accessToken = getAccessToken(IN_SIXTY_SECONDS);

        assertFalse(accessToken.isExpired());
    }

    private AccessToken getAccessToken(final long expiresInSeconds) {
        return new AccessToken(
                ACCESS_TOKEN,
                REFRESH_TOKEN,
                TOKEN_TYPE,
                expiresInSeconds,
                SCOPES
        );
    }
}
