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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OtpAuthenticationProviderTest {

    private final static String UI_EXTENSION_AUTHENTICATED_USER = "ui-extension-token-authenticated-user";
    private final static String UI_EXTENSION_TOKEN = "ui-extension-token";

    private final static String DOWNLOAD_AUTHENTICATED_USER = "download-token-authenticated-user";
    private final static String DOWNLOAD_TOKEN = "download-token";

    private OtpService otpService;
    private OtpAuthenticationProvider otpAuthenticationProvider;
    private NiFiProperties nifiProperties;

    @Before
    public void setUp() throws Exception {
        otpService = mock(OtpService.class);
        doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String downloadToken = (String) args[0];

                if (DOWNLOAD_TOKEN.equals(downloadToken)) {
                    return DOWNLOAD_AUTHENTICATED_USER;
                }

                throw new OtpAuthenticationException("Invalid token");
            }
        }).when(otpService).getAuthenticationFromDownloadToken(anyString());
        doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String uiExtensionToken = (String) args[0];

                if (UI_EXTENSION_TOKEN.equals(uiExtensionToken)) {
                    return UI_EXTENSION_AUTHENTICATED_USER;
                }

                throw new OtpAuthenticationException("Invalid token");
            }
        }).when(otpService).getAuthenticationFromUiExtensionToken(anyString());

        otpAuthenticationProvider = new OtpAuthenticationProvider(otpService, mock(NiFiProperties.class), mock(Authorizer.class));
    }

    @Test
    public void testUiExtensionPath() throws Exception {
        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(UI_EXTENSION_TOKEN, false, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(UI_EXTENSION_AUTHENTICATED_USER, details.getUsername());

        verify(otpService, times(1)).getAuthenticationFromUiExtensionToken(UI_EXTENSION_TOKEN);
        verify(otpService, never()).getAuthenticationFromDownloadToken(anyString());
    }

    @Test
    public void testDownload() throws Exception {
        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(DOWNLOAD_TOKEN, true, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, details.getUsername());

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

}
