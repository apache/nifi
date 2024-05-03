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
package org.apache.nifi.web.security.anonymous;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiAnonymousAuthenticationProviderTest {

    @Test
    public void testAnonymousDisabledNotSecure() {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(false);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(false, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }

    @Test
    public void testAnonymousEnabledNotSecure() {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(true);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(false, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }

    @Test
    public void testAnonymousDisabledSecure() {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(false);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(true, StringUtils.EMPTY);

        assertThrows(InvalidAuthenticationException.class, () -> anonymousAuthenticationProvider.authenticate(authenticationRequest));
    }

    @Test
    public void testAnonymousEnabledSecure() {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.isAnonymousAuthenticationAllowed()).thenReturn(true);

        final NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider = new NiFiAnonymousAuthenticationProvider(nifiProperties, mock(Authorizer.class));

        final NiFiAnonymousAuthenticationRequestToken authenticationRequest = new NiFiAnonymousAuthenticationRequestToken(true, StringUtils.EMPTY);

        final NiFiAuthenticationToken authentication = (NiFiAuthenticationToken) anonymousAuthenticationProvider.authenticate(authenticationRequest);
        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getDetails();
        assertTrue(userDetails.getNiFiUser().isAnonymous());
    }
}
