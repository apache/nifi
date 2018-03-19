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

package org.apache.nifi.oauth.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.oltu.oauth2.client.HttpClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthAccessTokenResponse;
import org.apache.oltu.oauth2.client.response.OAuthClientResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.apache.oltu.oauth2.common.utils.OAuthUtils;
import org.json.JSONObject;

public class OAuthHTTPConnectionClient
    implements HttpClient {

    private String accessTokenName = null;
    private String tokenTypeName = null;
    private String scopeName = null;
    private String expireInName = null;
    private String expireTimeName = null;

    public OAuthHTTPConnectionClient(String accessTokenName, String tokenTypeName, String scopeName, String expireInName, String expireTimeName) {
        this.accessTokenName = accessTokenName;
        this.tokenTypeName = tokenTypeName;
        this.scopeName = scopeName;
        this.expireInName = expireInName;
        this.expireTimeName = expireTimeName;
    }

    @Override
    public <T extends OAuthClientResponse> T execute(OAuthClientRequest request, Map<String, String> headers,
            String requestMethod, Class<T> responseClass) throws OAuthSystemException, OAuthProblemException {

        InputStream responseBody = null;
        URLConnection c;
        Map<String, List<String>> responseHeaders = new HashMap<String, List<String>>();
        int responseCode;
        try {
            String locURI = request.getLocationUri();
            if (locURI != null && locURI.startsWith("https")) {
                URL url = new URL(request.getLocationUri());

                c = url.openConnection();
                responseCode = -1;
                if (c instanceof HttpURLConnection) {
                    HttpURLConnection httpURLConnection = (HttpURLConnection) c;

                    if (headers != null && !headers.isEmpty()) {
                        for (Map.Entry<String, String> header : headers.entrySet()) {
                            httpURLConnection.addRequestProperty(header.getKey(), header.getValue());
                        }
                    }

                    if (request.getHeaders() != null) {
                        for (Map.Entry<String, String> header : request.getHeaders().entrySet()) {
                            httpURLConnection.addRequestProperty(header.getKey(), header.getValue());
                        }
                    }

                    if (OAuthUtils.isEmpty(requestMethod)) {
                        httpURLConnection.setRequestMethod(OAuth.HttpMethod.GET);
                    } else {
                        httpURLConnection.setRequestMethod(requestMethod);
                        setRequestBody(request, requestMethod, httpURLConnection);
                    }

                    httpURLConnection.connect();

                    InputStream inputStream;
                    responseCode = httpURLConnection.getResponseCode();
                    if (responseCode == 400 || responseCode == 405 || responseCode == 401 || responseCode == 403) {
                        inputStream = httpURLConnection.getErrorStream();
                    } else {
                        inputStream = httpURLConnection.getInputStream();
                    }

                    responseHeaders = httpURLConnection.getHeaderFields();
                    responseBody = inputStream;
                }
            } else {
                throw new OAuthSystemException("OAuth authentication endpoint " + request.getLocationUri() + " is not " +
                        "HTTPS. HTTPS is required for an OAuth authentication endpoint to be valid");
            }
        } catch (IOException e) {
            throw new OAuthSystemException(e);
        }

        CustomOAuthAccessTokenResponse cr = new CustomOAuthAccessTokenResponse(responseBody, c.getContentType(), responseCode, responseHeaders,
                accessTokenName, tokenTypeName, scopeName, expireInName, expireTimeName);

        return (T) cr;
    }

    private void setRequestBody(OAuthClientRequest request, String requestMethod, HttpURLConnection httpURLConnection)
            throws IOException {
        String requestBody = request.getBody();
        if (OAuthUtils.isEmpty(requestBody)) {
            return;
        }

        if (OAuth.HttpMethod.POST.equals(requestMethod) || OAuth.HttpMethod.PUT.equals(requestMethod)) {
            httpURLConnection.setDoOutput(true);
            OutputStream ost = httpURLConnection.getOutputStream();
            PrintWriter pw = new PrintWriter(ost);
            pw.print(requestBody);
            pw.flush();
            pw.close();
        }
    }

    @Override
    public void shutdown() {
        // Nothing to do here
    }


    public static class CustomOAuthAccessTokenResponse
        extends OAuthAccessTokenResponse {

        private String accessToken;
        private int responseCode;
        private String tokenType;
        private long expireTime;
        private long expiresIn;
        private String scope;

        public CustomOAuthAccessTokenResponse(InputStream responseBody, String contentType, int responseCode,
                Map<String, List<String>> responseHeaders, String accessTokenName, String tokenTypeName, String scopeName,
                String expireInName, String expireTimeName) throws OAuthProblemException {

            setResponseCode(responseCode);
            this.responseCode = responseCode;
            setContentType(contentType);
            this.contentType = contentType;
            setHeaders(responseHeaders);

            String loginResponseBody = null;

            if (responseBody != null) {
                StringWriter writer = new StringWriter();
                try {
                    IOUtils.copy(responseBody, writer, "UTF-8");
                } catch (IOException e) {
                    throw OAuthProblemException.error("Invalid response body received from access token request", e.getMessage());
                }
                loginResponseBody = writer.toString();
                JSONObject json = new JSONObject(loginResponseBody);

                if (json.has(accessTokenName)) {
                    this.accessToken = json.getString(accessTokenName);
                }

                if (json.has(tokenTypeName)) {
                    this.tokenType = json.getString(tokenTypeName);
                }

                if (json.has(expireTimeName)) {
                    this.expireTime =  json.getLong(expireTimeName);
                }

                if (json.has(expireInName)) {
                    this.expiresIn = json.getLong(expireInName);
                }

                if (json.has(scopeName)) {
                    this.scope = json.getString(scopeName);
                }
            }
        }

        @Override
        public String getAccessToken() {
            return this.accessToken;
        }

        @Override
        public String getTokenType() {
            return this.tokenType;
        }

        @Override
        public Long getExpiresIn() {
            return this.expiresIn;
        }

        @Override
        public String getRefreshToken() {
            return null;
        }

        @Override
        public String getScope() {
            return this.scope;
        }

        @Override
        public OAuthToken getOAuthToken() {
            BasicOAuthToken oAuthToken = new BasicOAuthToken(getAccessToken(), getTokenType(), getExpiresIn(), getRefreshToken(), getScope());
            return oAuthToken;
        }

        @Override
        protected void setContentType(String contentType) {
            this.contentType = contentType;
        }

        @Override
        protected void setResponseCode(int responseCode) {
            this.responseCode = responseCode;
        }
    }
}
