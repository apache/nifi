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

package org.apache.nifi.extensions;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.stateless.config.SslConfigurationUtil;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class NexusExtensionClient implements ExtensionClient {
    private static final Logger logger = LoggerFactory.getLogger(NexusExtensionClient.class);
    private static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final Charset URL_CHARSET = StandardCharsets.UTF_8;

    private final String baseUrl;
    private final long timeoutMillis;
    private final SslContextDefinition sslContextDefinition;

    public NexusExtensionClient(final String baseUrl, final SslContextDefinition sslContextDefinition, final String timeout) {
        this.baseUrl = baseUrl;
        this.sslContextDefinition = sslContextDefinition;
        this.timeoutMillis = timeout == null ? DEFAULT_TIMEOUT_MILLIS : FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public InputStream getExtension(final BundleCoordinate bundleCoordinate) throws IOException {
        final String url = resolveUrl(bundleCoordinate);
        logger.debug("Attempting to fetch {} from {}", bundleCoordinate, url);

        final WebClientService webClientService = getWebClientService();
        final URI uri = URI.create(url);
        final HttpResponseEntity responseEntity = webClientService.get().uri(uri).retrieve();
        final int statusCode = responseEntity.statusCode();

        if (statusCode == HttpURLConnection.HTTP_OK) {
            logger.debug("Successfully obtained stream for extension {} from {}", bundleCoordinate, url);
            final InputStream extensionByteStream = responseEntity.body();
            return new FilterInputStream(extensionByteStream) {
                @Override
                public void close() throws IOException {
                    responseEntity.close();
                    super.close();
                }
            };
        } else {
            try {
                if (statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    logger.debug("Received NOT FOUND response for extension {} from {}", bundleCoordinate, url);
                    return null;
                } else {
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    final InputStream body = responseEntity.body();
                    body.transferTo(outputStream);

                    final String responseBody = outputStream.toString();
                    final String message = "Failed to fetch extension %s from [%s] HTTP %d %s".formatted(bundleCoordinate, url, statusCode, responseBody);
                    throw new IOException(message);
                }
            } finally {
                responseEntity.close();
            }
        }
    }

    private WebClientService getWebClientService() throws IOException {
        final StandardWebClientService webClientService = new StandardWebClientService();

        final Duration timeout = Duration.ofMillis(timeoutMillis);
        webClientService.setConnectTimeout(timeout);
        webClientService.setReadTimeout(timeout);
        webClientService.setRedirectHandling(RedirectHandling.FOLLOWED);

        if (sslContextDefinition != null) {
            try {
                final TlsContext tlsContext = SslConfigurationUtil.createTlsContext(sslContextDefinition);
                webClientService.setTlsContext(tlsContext);
            } catch (final StatelessConfigurationException e) {
                throw new IOException("Web Client Service TLS Configuration failed", e);
            }
        }

        return webClientService;
    }

    private String resolveUrl(final BundleCoordinate bundleCoordinate) throws UnsupportedEncodingException {
        final StringBuilder sb = new StringBuilder(baseUrl);
        if (!baseUrl.endsWith("/")) {
            sb.append("/");
        }

        final String artifactPath = URLEncoder.encode(bundleCoordinate.getId(), URL_CHARSET);
        final String versionPath = URLEncoder.encode(bundleCoordinate.getVersion(), URL_CHARSET);

        sb.append(URLEncoder.encode(bundleCoordinate.getGroup(), URL_CHARSET).replace(".", "/"));
        sb.append("/");
        sb.append(artifactPath);
        sb.append("/");
        sb.append(versionPath);
        sb.append("/");
        sb.append(artifactPath).append("-").append(versionPath);
        sb.append(".nar");

        return sb.toString();
    }

    public String toString() {
        return "NexusExtensionClient[baseUrl=" + baseUrl + "]";
    }
}
