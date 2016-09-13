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
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityRequest;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityResponse;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.MessageDigest;
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
    private final Logger logger = LoggerFactory.getLogger(TlsCertificateSigningRequestPerformer.class);
    private final Supplier<HttpClientBuilder> httpClientBuilderSupplier;
    private final String caHostname;
    private final String dn;
    private final String token;
    private final int port;
    private final ObjectMapper objectMapper;
    private final String signingAlgorithm;

    public TlsCertificateSigningRequestPerformer(TlsClientConfig tlsClientConfig) throws NoSuchAlgorithmException {
        this(HttpClientBuilder::create, tlsClientConfig.getCaHostname(), tlsClientConfig.getDn(), tlsClientConfig.getToken(), tlsClientConfig.getPort(), tlsClientConfig.getSigningAlgorithm());
    }

    protected TlsCertificateSigningRequestPerformer(Supplier<HttpClientBuilder> httpClientBuilderSupplier, TlsClientConfig tlsClientConfig) throws NoSuchAlgorithmException {
        this(httpClientBuilderSupplier, tlsClientConfig.getCaHostname(), tlsClientConfig.getDn(), tlsClientConfig.getToken(), tlsClientConfig.getPort(), tlsClientConfig.getSigningAlgorithm());
    }

    private TlsCertificateSigningRequestPerformer(Supplier<HttpClientBuilder> httpClientBuilderSupplier, String caHostname, String dn, String token, int port, String signingAlgorithm) {
        this.httpClientBuilderSupplier = httpClientBuilderSupplier;
        this.caHostname = caHostname;
        this.dn = CertificateUtils.reorderDn(dn);
        this.token = token;
        this.port = port;
        this.objectMapper = new ObjectMapper();
        this.signingAlgorithm = signingAlgorithm;
    }

    /**
     * Submits a CSR to the Certificate authority, checks the resulting hmac, and returns the chain if everything succeeds
     *
     * @param keyPair the keypair to generate the csr for
     * @throws IOException if there is a problem during the process
     * @returnd the resulting certificate chain
     */
    public X509Certificate[] perform(KeyPair keyPair) throws IOException {
        try {
            List<X509Certificate> certificates = new ArrayList<>();

            HttpClientBuilder httpClientBuilder = httpClientBuilderSupplier.get();
            SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
            sslContextBuilder.useProtocol("TLSv1.2");

            // We will be validating that we are talking to the correct host once we get the response's hmac of the token and public key of the ca
            sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            httpClientBuilder.setSSLSocketFactory(new TlsCertificateAuthorityClientSocketFactory(sslContextBuilder.build(), caHostname, certificates));

            String jsonResponseString;
            int responseCode;
            try (CloseableHttpClient client = httpClientBuilder.build()) {
                JcaPKCS10CertificationRequest request = TlsHelper.generateCertificationRequest(dn, keyPair, signingAlgorithm);
                TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest = new TlsCertificateAuthorityRequest(TlsHelper.calculateHMac(token, request.getPublicKey()),
                        TlsHelper.pemEncodeJcaObject(request));

                HttpPost httpPost = new HttpPost();
                httpPost.setEntity(new ByteArrayEntity(objectMapper.writeValueAsBytes(tlsCertificateAuthorityRequest)));

                if (logger.isInfoEnabled()) {
                    logger.info("Requesting certificate with dn " + dn + " from " + caHostname + ":" + port);
                }
                try (CloseableHttpResponse response = client.execute(new HttpHost(caHostname, port, "https"), httpPost)) {
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
            byte[] expectedHmac = TlsHelper.calculateHMac(token, caCertificate.getPublicKey());

            if (!MessageDigest.isEqual(expectedHmac, tlsCertificateAuthorityResponse.getHmac())) {
                throw new IOException(UNEXPECTED_HMAC_RECEIVED_POSSIBLE_MAN_IN_THE_MIDDLE);
            }

            if (!tlsCertificateAuthorityResponse.hasCertificate()) {
                throw new IOException(EXPECTED_RESPONSE_TO_CONTAIN_CERTIFICATE);
            }
            X509Certificate x509Certificate = TlsHelper.parseCertificate(new StringReader(tlsCertificateAuthorityResponse.getPemEncodedCertificate()));
            x509Certificate.verify(caCertificate.getPublicKey());
            if (logger.isInfoEnabled()) {
                logger.info("Got certificate with dn " + x509Certificate.getSubjectX500Principal());
            }
            return new X509Certificate[]{x509Certificate, caCertificate};
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
