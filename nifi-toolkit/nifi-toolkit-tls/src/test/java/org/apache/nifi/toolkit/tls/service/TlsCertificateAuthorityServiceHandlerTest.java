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

package org.apache.nifi.toolkit.tls.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.cert.crmf.CRMFException;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TlsCertificateAuthorityServiceHandlerTest {
    @Mock
    TlsHelper tlsHelper;

    @Mock
    X509Certificate caCert;

    @Mock
    PrivateKey privateKey;

    @Mock
    PublicKey publicKey;

    @Mock
    Request baseRequest;

    @Mock
    HttpServletRequest httpServletRequest;

    @Mock
    HttpServletResponse httpServletResponse;

    @Mock
    JcaPKCS10CertificationRequest jcaPKCS10CertificationRequest;

    @Mock
    PublicKey certificateRequestPublicKey;

    @Mock
    X509Certificate signedCsr;

    KeyPair keyPair;

    String testToken;

    byte[] testHmac;

    byte[] testCaHmac;

    String testPemEncodedCsr;

    String testPemEncodedSignedCertificate;

    ObjectMapper objectMapper;

    TlsCertificateAuthorityServiceHandler tlsCertificateAuthorityServiceHandler;

    TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest;

    int statusCode;

    StringWriter response;

    @Before
    public void setup() throws Exception {
        testToken = "testToken";
        testHmac = "testHmac".getBytes(StandardCharsets.UTF_8);
        testCaHmac = "testCaHmac".getBytes(StandardCharsets.UTF_8);
        testPemEncodedCsr = "testPemEncodedCsr";
        testPemEncodedSignedCertificate = "testPemEncodedSignedCertificate";
        keyPair = new KeyPair(publicKey, privateKey);
        objectMapper = new ObjectMapper();
        when(httpServletRequest.getReader()).thenAnswer(invocation -> {
            StringWriter stringWriter = new StringWriter();
            objectMapper.writeValue(stringWriter, tlsCertificateAuthorityRequest);
            return new BufferedReader(new StringReader(stringWriter.toString()));
        });
        doAnswer(invocation -> statusCode = (int) invocation.getArguments()[0]).when(httpServletResponse).setStatus(anyInt());
        doAnswer(invocation -> {
            statusCode = (int)invocation.getArguments()[0];
            StringWriter stringWriter = new StringWriter();
            stringWriter.write((String) invocation.getArguments()[1]);
            response = stringWriter;
            return null;
        }).when(httpServletResponse).sendError(anyInt(), anyString());
        when(httpServletResponse.getWriter()).thenAnswer(invocation -> {
            response = new StringWriter();
            return new PrintWriter(response);
        });
        when(tlsHelper.parseCsr(testPemEncodedCsr)).thenReturn(jcaPKCS10CertificationRequest);
        when(tlsHelper.signCsr(jcaPKCS10CertificationRequest, caCert, keyPair)).thenReturn(signedCsr);
        when(tlsHelper.pemEncodeJcaObject(signedCsr)).thenReturn(testPemEncodedSignedCertificate);
        when(jcaPKCS10CertificationRequest.getPublicKey()).thenReturn(certificateRequestPublicKey);
        when(tlsHelper.calculateHMac(testToken, caCert.getPublicKey())).thenReturn(testCaHmac);
        tlsCertificateAuthorityServiceHandler = new TlsCertificateAuthorityServiceHandler(tlsHelper, testToken, caCert, keyPair, objectMapper);
    }

    private TlsCertificateAuthorityResponse getResponse() throws IOException {
        return objectMapper.readValue(new StringReader(response.toString()), TlsCertificateAuthorityResponse.class);
    }

    @Test
    public void testSuccess() throws IOException, ServletException, NoSuchAlgorithmException, CRMFException, NoSuchProviderException, InvalidKeyException {
        when(tlsHelper.checkHMac(testHmac, testToken, certificateRequestPublicKey)).thenReturn(true);
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(testHmac, testPemEncodedCsr);
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
        assertEquals(Response.SC_OK, statusCode);
        assertArrayEquals(testCaHmac, getResponse().getHmac());
        assertEquals(testPemEncodedSignedCertificate, getResponse().getPemEncodedCertificate());
    }

    @Test
    public void testNoCsr() throws IOException, ServletException {
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(testHmac, null);
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
        assertEquals(Response.SC_BAD_REQUEST, statusCode);
        assertEquals(TlsCertificateAuthorityServiceHandler.CSR_FIELD_MUST_BE_SET, getResponse().getError());
    }

    @Test
    public void testNoHmac() throws IOException, ServletException {
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(null, testPemEncodedCsr);
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
        assertEquals(Response.SC_BAD_REQUEST, statusCode);
        assertEquals(TlsCertificateAuthorityServiceHandler.HMAC_FIELD_MUST_BE_SET, getResponse().getError());
    }

    @Test
    public void testForbidden() throws IOException, ServletException, NoSuchAlgorithmException, CRMFException, NoSuchProviderException, InvalidKeyException {
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(testHmac, testPemEncodedCsr);
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
        assertEquals(Response.SC_FORBIDDEN, statusCode);
        assertEquals(TlsCertificateAuthorityServiceHandler.FORBIDDEN, getResponse().getError());
    }

    @Test(expected = ServletException.class)
    public void testServletException() throws IOException, ServletException {
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
    }

    @After
    public void verifyHandled() {
        verify(baseRequest).setHandled(true);
    }
}
