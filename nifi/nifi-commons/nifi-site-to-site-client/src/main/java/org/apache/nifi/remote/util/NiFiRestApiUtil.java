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
package org.apache.nifi.remote.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class NiFiRestApiUtil {

    public static final int RESPONSE_CODE_OK = 200;

    private final SSLContext sslContext;

    public NiFiRestApiUtil(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    private HttpURLConnection getConnection(final String connUrl, final int timeoutMillis) throws IOException {
        final URL url = new URL(connUrl);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(timeoutMillis);
        connection.setReadTimeout(timeoutMillis);

        // special handling for https
        if (sslContext != null && connection instanceof HttpsURLConnection) {
            HttpsURLConnection secureConnection = (HttpsURLConnection) connection;
            secureConnection.setSSLSocketFactory(sslContext.getSocketFactory());

            // check the trusted hostname property and override the HostnameVerifier
            secureConnection.setHostnameVerifier(new OverrideHostnameVerifier(url.getHost(),
                    secureConnection.getHostnameVerifier()));
        }

        return connection;
    }

    public ControllerDTO getController(final String url, final int timeoutMillis) throws IOException {
        final HttpURLConnection connection = getConnection(url, timeoutMillis);
        connection.setRequestMethod("GET");
        final int responseCode = connection.getResponseCode();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamUtils.copy(connection.getInputStream(), baos);
        final String responseMessage = baos.toString();

        if (responseCode == RESPONSE_CODE_OK) {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode jsonNode = mapper.readTree(responseMessage);
            final JsonNode controllerNode = jsonNode.get("controller");
            return mapper.readValue(controllerNode, ControllerDTO.class);
        } else {
            throw new IOException("Got HTTP response Code " + responseCode + ": " + connection.getResponseMessage() + " with explanation: " + responseMessage);
        }
    }

    private static class OverrideHostnameVerifier implements HostnameVerifier {

        private final String trustedHostname;
        private final HostnameVerifier delegate;

        private OverrideHostnameVerifier(String trustedHostname, HostnameVerifier delegate) {
            this.trustedHostname = trustedHostname;
            this.delegate = delegate;
        }

        @Override
        public boolean verify(String hostname, SSLSession session) {
            if (trustedHostname.equalsIgnoreCase(hostname)) {
                return true;
            }
            return delegate.verify(hostname, session);
        }
    }
}
