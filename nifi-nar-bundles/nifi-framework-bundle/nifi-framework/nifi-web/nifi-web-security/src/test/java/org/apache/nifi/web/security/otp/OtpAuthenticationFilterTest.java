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

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.NiFiAuthorizationRequestToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.servlet.http.HttpServletRequest;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OtpAuthenticationFilterTest {

    private final static String UI_EXTENSION_AUTHENTICATED_USER = "ui-extension-token-authenticated-user";
    private final static String UI_EXTENSION_TOKEN = "ui-extension-token";

    private final static String DOWNLOAD_AUTHENTICATED_USER = "download-token-authenticated-user";
    private final static String DOWNLOAD_TOKEN = "download-token";

    private OtpService otpService;
    private OtpAuthenticationFilter otpAuthenticationFilter;

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

        otpAuthenticationFilter = new OtpAuthenticationFilter();
        otpAuthenticationFilter.setOtpService(otpService);
    }

    @Test
    public void testInsecureHttp() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(false);
        assertNull(otpAuthenticationFilter.attemptAuthentication(request));
    }

    @Test
    public void testNoAccessToken() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(null);
        assertNull(otpAuthenticationFilter.attemptAuthentication(request));
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testTokenSupportDisabled() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn("my-access-token");

        final OtpAuthenticationFilter noTokenAuthenticationFilter = new OtpAuthenticationFilter();
        noTokenAuthenticationFilter.attemptAuthentication(request);
    }

    @Test
    public void testUnsupportedDownloadPath() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn("my-access-token");
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getPathInfo()).thenReturn("/controller/config");

        assertNull(otpAuthenticationFilter.attemptAuthentication(request));
    }

    @Test
    public void testUiExtensionPath() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(UI_EXTENSION_TOKEN);
        when(request.getContextPath()).thenReturn("/nifi-update-attribute-ui");

        final NiFiAuthorizationRequestToken result = otpAuthenticationFilter.attemptAuthentication(request);
        final List<String> chain = result.getChain();
        assertEquals(1, chain.size());
        assertEquals(UI_EXTENSION_AUTHENTICATED_USER, chain.get(0));

        verify(otpService, times(1)).getAuthenticationFromUiExtensionToken(UI_EXTENSION_TOKEN);
        verify(otpService, never()).getAuthenticationFromDownloadToken(anyString());
    }

    @Test
    public void testProvenanceInputContentDownload() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(DOWNLOAD_TOKEN);
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getPathInfo()).thenReturn("/controller/provenance/events/0/content/input");

        final NiFiAuthorizationRequestToken result = otpAuthenticationFilter.attemptAuthentication(request);
        final List<String> chain = result.getChain();
        assertEquals(1, chain.size());
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, chain.get(0));

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

    @Test
    public void testProvenanceOutputContentDownload() throws Exception {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(DOWNLOAD_TOKEN);
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getPathInfo()).thenReturn("/controller/provenance/events/0/content/output");

        final NiFiAuthorizationRequestToken result = otpAuthenticationFilter.attemptAuthentication(request);
        final List<String> chain = result.getChain();
        assertEquals(1, chain.size());
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, chain.get(0));

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

    @Test
    public void testFlowFileContentDownload() throws Exception {
        final String uuid = UUID.randomUUID().toString();

        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(DOWNLOAD_TOKEN);
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getPathInfo()).thenReturn(String.format("/controller/process-groups/root/connections/%s/flowfiles/%s/content", uuid, uuid));

        final NiFiAuthorizationRequestToken result = otpAuthenticationFilter.attemptAuthentication(request);
        final List<String> chain = result.getChain();
        assertEquals(1, chain.size());
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, chain.get(0));

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

    @Test
    public void testTemplateDownload() throws Exception {
        final String uuid = UUID.randomUUID().toString();

        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getParameter(OtpAuthenticationFilter.ACCESS_TOKEN)).thenReturn(DOWNLOAD_TOKEN);
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getPathInfo()).thenReturn(String.format("/controller/templates/%s", uuid));

        final NiFiAuthorizationRequestToken result = otpAuthenticationFilter.attemptAuthentication(request);
        final List<String> chain = result.getChain();
        assertEquals(1, chain.size());
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, chain.get(0));

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

}
