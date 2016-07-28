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
import org.apache.commons.io.input.BoundedReader;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityRequest;
import org.apache.nifi.toolkit.tls.service.dto.TlsCertificateAuthorityResponse;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;

/**
 * Jetty service handler that validates the hmac of a CSR and issues a certificate if it checks out
 */
public class TlsCertificateAuthorityServiceHandler extends AbstractHandler {
    public static final String CSR_FIELD_MUST_BE_SET = "csr field must be set";
    public static final String HMAC_FIELD_MUST_BE_SET = "hmac field must be set";
    public static final String FORBIDDEN = "forbidden";
    private final TlsHelper tlsHelper;
    private final String token;
    private final X509Certificate caCert;
    private final KeyPair keyPair;
    private final ObjectMapper objectMapper;

    public TlsCertificateAuthorityServiceHandler(TlsHelper tlsHelper, String token, X509Certificate caCert, KeyPair keyPair, ObjectMapper objectMapper) {
        this.tlsHelper = tlsHelper;
        this.token = token;
        this.caCert = caCert;
        this.keyPair = keyPair;
        this.objectMapper = objectMapper;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            TlsCertificateAuthorityRequest tlsCertificateAuthorityRequest = objectMapper.readValue(new BoundedReader(request.getReader(), 1024 * 1024), TlsCertificateAuthorityRequest.class);

            if (!tlsCertificateAuthorityRequest.hasHmac()) {
                writeResponse(objectMapper, response, new TlsCertificateAuthorityResponse(HMAC_FIELD_MUST_BE_SET), Response.SC_BAD_REQUEST);
                return;
            }

            if (!tlsCertificateAuthorityRequest.hasCsr()) {
                writeResponse(objectMapper, response, new TlsCertificateAuthorityResponse(CSR_FIELD_MUST_BE_SET), Response.SC_BAD_REQUEST);
                return;
            }

            JcaPKCS10CertificationRequest jcaPKCS10CertificationRequest = tlsHelper.parseCsr(tlsCertificateAuthorityRequest.getCsr());
            byte[] expectedHmac = tlsHelper.calculateHMac(token, jcaPKCS10CertificationRequest.getPublicKey());

            if (MessageDigest.isEqual(expectedHmac, tlsCertificateAuthorityRequest.getHmac())) {
                X509Certificate x509Certificate = tlsHelper.signCsr(jcaPKCS10CertificationRequest, this.caCert, keyPair);
                writeResponse(objectMapper, response, new TlsCertificateAuthorityResponse(tlsHelper.calculateHMac(token, caCert.getPublicKey()),
                        tlsHelper.pemEncodeJcaObject(x509Certificate)), Response.SC_OK);
                return;
            } else {
                writeResponse(objectMapper, response, new TlsCertificateAuthorityResponse(FORBIDDEN), Response.SC_FORBIDDEN);
                return;
            }
        } catch (Exception e) {
            throw new ServletException("Server error");
        } finally {
            baseRequest.setHandled(true);
        }
    }

    private void writeResponse(ObjectMapper objectMapper, HttpServletResponse response, TlsCertificateAuthorityResponse tlsCertificateAuthorityResponse, int responseCode) throws IOException {
        if (responseCode == Response.SC_OK) {
            objectMapper.writeValue(response.getWriter(), tlsCertificateAuthorityResponse);
            response.setStatus(responseCode);
        } else {
            response.sendError(responseCode, objectMapper.writeValueAsString(tlsCertificateAuthorityResponse));
        }
    }
}
