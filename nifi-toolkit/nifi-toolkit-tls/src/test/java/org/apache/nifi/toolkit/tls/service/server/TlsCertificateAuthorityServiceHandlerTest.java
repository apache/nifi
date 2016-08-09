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

package org.apache.nifi.toolkit.tls.service.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityRequest;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityResponse;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.crmf.CRMFException;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
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
    X509Certificate caCert;

    @Mock
    Request baseRequest;

    @Mock
    HttpServletRequest httpServletRequest;

    @Mock
    HttpServletResponse httpServletResponse;

    JcaPKCS10CertificationRequest jcaPKCS10CertificationRequest;

    KeyPair keyPair;

    String testToken;

    String testPemEncodedCsr;

    String testPemEncodedSignedCertificate;

    ObjectMapper objectMapper;

    TlsCertificateAuthorityServiceHandler tlsCertificateAuthorityServiceHandler;

    TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest;

    int statusCode;

    StringWriter response;
    private byte[] testCaHmac;
    private byte[] testHmac;
    private String requestedDn;
    private KeyPair certificateKeyPair;

    @BeforeClass
    public static void before() {
        TlsHelper.addBouncyCastleProvider();
    }

    @Before
    public void setup() throws Exception {
        testToken = "testToken";
        testPemEncodedSignedCertificate = "testPemEncodedSignedCertificate";
        keyPair = TlsHelper.generateKeyPair(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM, TlsConfig.DEFAULT_KEY_SIZE);
        objectMapper = new ObjectMapper();
        when(httpServletRequest.getReader()).thenAnswer(invocation -> {
            StringWriter stringWriter = new StringWriter();
            objectMapper.writeValue(stringWriter, tlsCertificateAuthorityRequest);
            return new BufferedReader(new StringReader(stringWriter.toString()));
        });
        doAnswer(invocation -> statusCode = (int) invocation.getArguments()[0]).when(httpServletResponse).setStatus(anyInt());
        doAnswer(invocation -> {
            statusCode = (int) invocation.getArguments()[0];
            StringWriter stringWriter = new StringWriter();
            stringWriter.write((String) invocation.getArguments()[1]);
            response = stringWriter;
            return null;
        }).when(httpServletResponse).sendError(anyInt(), anyString());
        when(httpServletResponse.getWriter()).thenAnswer(invocation -> {
            response = new StringWriter();
            return new PrintWriter(response);
        });
        caCert = CertificateUtils.generateSelfSignedX509Certificate(keyPair, "CN=fakeCa", TlsConfig.DEFAULT_SIGNING_ALGORITHM, TlsConfig.DEFAULT_DAYS);
        requestedDn = TlsConfig.calcDefaultDn(TlsConfig.DEFAULT_HOSTNAME);
        certificateKeyPair = TlsHelper.generateKeyPair(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM, TlsConfig.DEFAULT_KEY_SIZE);
        jcaPKCS10CertificationRequest = TlsHelper.generateCertificationRequest(requestedDn, certificateKeyPair, TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        testPemEncodedCsr = TlsHelper.pemEncodeJcaObject(jcaPKCS10CertificationRequest);
        tlsCertificateAuthorityServiceHandler = new TlsCertificateAuthorityServiceHandler(TlsConfig.DEFAULT_SIGNING_ALGORITHM, TlsConfig.DEFAULT_DAYS, testToken, caCert, keyPair, objectMapper);
        testHmac = TlsHelper.calculateHMac(testToken, jcaPKCS10CertificationRequest.getPublicKey());
        testCaHmac = TlsHelper.calculateHMac(testToken, caCert.getPublicKey());
    }

    private TlsCertificateAuthorityResponse getResponse() throws IOException {
        return objectMapper.readValue(new StringReader(response.toString()), TlsCertificateAuthorityResponse.class);
    }

    @Test
    public void testSuccess() throws IOException, ServletException, GeneralSecurityException, CRMFException {
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(testHmac, testPemEncodedCsr);
        tlsCertificateAuthorityServiceHandler.handle(null, baseRequest, httpServletRequest, httpServletResponse);
        assertEquals(Response.SC_OK, statusCode);
        assertArrayEquals(testCaHmac, getResponse().getHmac());
        X509Certificate certificate = TlsHelper.parseCertificate(new StringReader(getResponse().getPemEncodedCertificate()));
        assertEquals(certificateKeyPair.getPublic(), certificate.getPublicKey());
        assertEquals(new X500Name(requestedDn), new X500Name(certificate.getSubjectDN().toString()));
        certificate.verify(caCert.getPublicKey());
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
        tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest("badHmac".getBytes(StandardCharsets.UTF_8), testPemEncodedCsr);
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
