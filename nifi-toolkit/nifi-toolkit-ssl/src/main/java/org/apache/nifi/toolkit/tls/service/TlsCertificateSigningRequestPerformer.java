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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class TlsCertificateSigningRequestPerformer {
    public static final String RECEIVED_RESPONSE_CODE = "Received response code ";
    public static final String EXPECTED_ONE_CERTIFICATE = "Expected one certificate";
    public static final String EXPECTED_RESPONSE_TO_CONTAIN_HMAC = "Expected response to contain hmac";
    public static final String UNEXPECTED_HMAC_RECEIVED_POSSIBLE_MAN_IN_THE_MIDDLE = "Unexpected hmac received, possible man in the middle";
    public static final String EXPECTED_RESPONSE_TO_CONTAIN_CERTIFICATE = "Expected response to contain certificate";
    private final Supplier<HttpClientBuilder> httpClientBuilderSupplier;
    private final TlsClientConfig tlsClientConfig;

    public TlsCertificateSigningRequestPerformer(TlsClientConfig tlsClientConfig) throws NoSuchAlgorithmException {
        this(HttpClientBuilder::create, tlsClientConfig);
    }

    public TlsCertificateSigningRequestPerformer(Supplier<HttpClientBuilder> httpClientBuilderSupplier, TlsClientConfig tlsClientConfig) {
        this.httpClientBuilderSupplier = httpClientBuilderSupplier;
        this.tlsClientConfig = tlsClientConfig;
    }

    /**
     * Submits a CSR to the Certificate authority, checks the resulting hmac, and returns the chain if everything succeeds
     *
     * @param objectMapper for serialization
     * @param keyPair      the keypair to generate the csr for
     * @throws IOException if there is a problem during the process
     * @returnd the resulting certificate chain
     */
    public X509Certificate[] perform(ObjectMapper objectMapper, KeyPair keyPair) throws IOException {
        try {
            List<X509Certificate> certificates = new ArrayList<>();
            TlsHelper tlsHelper = tlsClientConfig.createTlsHelper();

            HttpClientBuilder httpClientBuilder = httpClientBuilderSupplier.get();
            SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();

            // We will be validating that we are talking to the correct host once we get the response's hmac of the token and public key of the ca
            sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            httpClientBuilder.setSSLSocketFactory(new TlsCertificateAuthorityClientSocketFactory(sslContextBuilder.build(), tlsClientConfig.getCaHostname(), certificates));

            String jsonResponseString;
            int responseCode;
            try (CloseableHttpClient client = httpClientBuilder.build()) {
                JcaPKCS10CertificationRequest request = tlsHelper.generateCertificationRequest("CN=" + tlsClientConfig.getHostname() + ",OU=NIFI", keyPair);
                TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(tlsHelper.calculateHMac(tlsClientConfig.getToken(), request.getPublicKey()),
                        tlsHelper.pemEncodeJcaObject(request));

                HttpPost httpPost = new HttpPost();
                httpPost.setEntity(new ByteArrayEntity(objectMapper.writeValueAsBytes(tlsCertificateAuthorityRequest)));

                try (CloseableHttpResponse response = client.execute(new HttpHost(tlsClientConfig.getCaHostname(), tlsClientConfig.getPort(), "https"), httpPost)) {
                    jsonResponseString = IOUtils.toString(new BoundedInputStream(response.getEntity().getContent(), 1024 * 1024), StandardCharsets.UTF_8);
                    responseCode = response.getStatusLine().getStatusCode();
                }
            }

            if (responseCode != Response.SC_OK) {
                throw new IOException(RECEIVED_RESPONSE_CODE + responseCode + " with payload " + jsonResponseString);
            }

            if (certificates.size() != 1) {
                throw new IOException(EXPECTED_ONE_CERTIFICATE);
            }

            TlsCertificateAuthorityResponse tlsCertificateAuthorityResponse = objectMapper.readValue(jsonResponseString, TlsCertificateAuthorityResponse.class);
            if (!tlsCertificateAuthorityResponse.hasHmac()) {
                throw new IOException(EXPECTED_RESPONSE_TO_CONTAIN_HMAC);
            }

            X509Certificate caCertificate = certificates.get(0);
            if (!tlsHelper.checkHMac(tlsCertificateAuthorityResponse.getHmac(), tlsClientConfig.getToken(), caCertificate.getPublicKey())) {
                throw new IOException(UNEXPECTED_HMAC_RECEIVED_POSSIBLE_MAN_IN_THE_MIDDLE);
            }

            if (!tlsCertificateAuthorityResponse.hasCertificate()) {
                throw new IOException(EXPECTED_RESPONSE_TO_CONTAIN_CERTIFICATE);
            }
            X509Certificate x509Certificate = tlsHelper.parseCertificate(tlsCertificateAuthorityResponse.getPemEncodedCertificate());
            x509Certificate.verify(caCertificate.getPublicKey());
            return new X509Certificate[]{x509Certificate, caCertificate};
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
