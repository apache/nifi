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

package org.apache.nifi.toolkit.tls.service.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityRequest;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityResponse;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TlsCertificateSigningRequestPerformerTest {
    @Mock
    Supplier<HttpClientBuilder> httpClientBuilderSupplier;

    @Mock
    HttpClientBuilder httpClientBuilder;

    @Mock
    CloseableHttpClient closeableHttpClient;

    @Mock
    TlsClientConfig tlsClientConfig;

    X509Certificate caCertificate;

    X509Certificate signedCsr;

    ObjectMapper objectMapper;
    KeyPair keyPair;
    TlsCertificateSigningRequestPerformer tlsCertificateSigningRequestPerformer;
    String testToken;
    String testCaHostname;
    int testPort;
    List<X509Certificate> certificates;

    TlsCertificateAuthorityResponse tlsCertificateAuthorityResponse;
    int statusCode;
    private byte[] testHmac;
    private String testSignedCsr;

    @Before
    public void setup() throws GeneralSecurityException, OperatorCreationException, IOException {
        objectMapper = new ObjectMapper();
        keyPair = TlsHelper.generateKeyPair(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM, TlsConfig.DEFAULT_KEY_SIZE);

        testToken = "testToken";
        testCaHostname = "testCaHostname";
        testPort = 8993;
        certificates = new ArrayList<>();

        when(tlsClientConfig.getToken()).thenReturn(testToken);
        when(tlsClientConfig.getCaHostname()).thenReturn(testCaHostname);
        when(tlsClientConfig.getDn()).thenReturn(new TlsConfig().calcDefaultDn(testCaHostname));
        when(tlsClientConfig.getPort()).thenReturn(testPort);
        when(tlsClientConfig.createCertificateSigningRequestPerformer()).thenReturn(tlsCertificateSigningRequestPerformer);
        when(tlsClientConfig.getSigningAlgorithm()).thenReturn(TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        JcaPKCS10CertificationRequest jcaPKCS10CertificationRequest = TlsHelper.generateCertificationRequest(tlsClientConfig.getDn(), keyPair, TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        String testCsrPem = TlsHelper.pemEncodeJcaObject(jcaPKCS10CertificationRequest);
        when(httpClientBuilderSupplier.get()).thenReturn(httpClientBuilder);
        when(httpClientBuilder.build()).thenAnswer(invocation -> {
            Field sslSocketFactory = HttpClientBuilder.class.getDeclaredField("sslSocketFactory");
            sslSocketFactory.setAccessible(true);
            Object o = sslSocketFactory.get(httpClientBuilder);
            Field field = TlsCertificateAuthorityClientSocketFactory.class.getDeclaredField("certificates");
            field.setAccessible(true);
            ((List<X509Certificate>) field.get(o)).addAll(certificates);
            return closeableHttpClient;
        });
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenAnswer(i -> statusCode);
        when(closeableHttpClient.execute(eq(new HttpHost(testCaHostname, testPort, "https")), any(HttpPost.class))).thenAnswer(invocation -> {
            HttpPost httpPost = (HttpPost) invocation.getArguments()[1];
            TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest = objectMapper.readValue(httpPost.getEntity().getContent(), TlsCertificateAuthorityRequest.class);
            assertEquals(tlsCertificateAuthorityRequest.getCsr(), testCsrPem);
            CloseableHttpResponse closeableHttpResponse = mock(CloseableHttpResponse.class);
            when(closeableHttpResponse.getEntity()).thenAnswer(i -> {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                objectMapper.writeValue(byteArrayOutputStream, tlsCertificateAuthorityResponse);
                return new ByteArrayEntity(byteArrayOutputStream.toByteArray());
            });
            when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
            return closeableHttpResponse;
        });
        KeyPair caKeyPair = TlsHelper.generateKeyPair(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM, TlsConfig.DEFAULT_KEY_SIZE);
        caCertificate = CertificateUtils.generateSelfSignedX509Certificate(caKeyPair, "CN=fakeCa", TlsConfig.DEFAULT_SIGNING_ALGORITHM, TlsConfig.DEFAULT_DAYS);
        testHmac = TlsHelper.calculateHMac(testToken, caCertificate.getPublicKey());
        signedCsr = CertificateUtils.generateIssuedCertificate(jcaPKCS10CertificationRequest.getSubject().toString(), jcaPKCS10CertificationRequest.getPublicKey(),
                caCertificate, caKeyPair, TlsConfig.DEFAULT_SIGNING_ALGORITHM, TlsConfig.DEFAULT_DAYS);
        testSignedCsr = TlsHelper.pemEncodeJcaObject(signedCsr);

        tlsCertificateSigningRequestPerformer = new TlsCertificateSigningRequestPerformer(httpClientBuilderSupplier, tlsClientConfig);
    }

    @Test
    public void testOk() throws Exception {
        certificates.add(caCertificate);
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse(testHmac, testSignedCsr);
        tlsCertificateSigningRequestPerformer.perform(keyPair);
    }

    @Test
    public void testBadStatusCode() throws Exception {
        statusCode = Response.SC_FORBIDDEN;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse();
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertTrue(e.getMessage().startsWith(TlsCertificateSigningRequestPerformer.RECEIVED_RESPONSE_CODE + statusCode));
        }
    }

    @Test
    public void test0CertSize() throws Exception {
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse();
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertEquals(TlsCertificateSigningRequestPerformer.EXPECTED_ONE_CERTIFICATE, e.getMessage());
        }
    }

    @Test
    public void test2CertSize() throws Exception {
        certificates.add(caCertificate);
        certificates.add(caCertificate);
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse();
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertEquals(TlsCertificateSigningRequestPerformer.EXPECTED_ONE_CERTIFICATE, e.getMessage());
        }
    }

    @Test
    public void testNoHmac() throws Exception {
        certificates.add(caCertificate);
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse(null, testSignedCsr);
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertEquals(TlsCertificateSigningRequestPerformer.EXPECTED_RESPONSE_TO_CONTAIN_HMAC, e.getMessage());
        }
    }

    @Test
    public void testBadHmac() throws Exception {
        certificates.add(caCertificate);
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse("badHmac".getBytes(StandardCharsets.UTF_8), testSignedCsr);
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertEquals(TlsCertificateSigningRequestPerformer.UNEXPECTED_HMAC_RECEIVED_POSSIBLE_MAN_IN_THE_MIDDLE, e.getMessage());
        }
    }

    @Test
    public void testNoCertificate() throws Exception {
        certificates.add(caCertificate);
        statusCode = Response.SC_OK;
        tlsCertificateAuthorityResponse = new TlsCertificateAuthorityResponse(testHmac, null);
        try {
            tlsCertificateSigningRequestPerformer.perform(keyPair);
            fail("Expected IOE");
        } catch (IOException e) {
            assertEquals(TlsCertificateSigningRequestPerformer.EXPECTED_RESPONSE_TO_CONTAIN_CERTIFICATE, e.getMessage());
        }
    }
}
