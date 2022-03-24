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

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.stateless.config.SslConfigurationUtil;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

public class NexusExtensionClient implements ExtensionClient {
    private static final Logger logger = LoggerFactory.getLogger(NexusExtensionClient.class);
    private static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final String URL_CHARSET = "UTF-8";

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

        final OkHttpClient okHttpClient = createClient();
        final Request request = new Request.Builder()
            .get()
            .url(url)
            .build();

        final Call call = okHttpClient.newCall(request);
        final Response response = call.execute();
        if (response.isSuccessful() && response.body() != null) {
            logger.debug("Successfully obtained stream for extension {} from {}", bundleCoordinate, url);
            final InputStream extensionByteStream = response.body().byteStream();
            return new FilterInputStream(extensionByteStream) {
                @Override
                public void close() throws IOException {
                    response.close();
                    super.close();
                }
            };
        } else {
            try {
                if (response.code() == javax.ws.rs.core.Response.Status.NOT_FOUND.getStatusCode()) {
                    logger.debug("Received NOT FOUND response for extension {} from {}", bundleCoordinate, url);
                    return null;
                } else {
                    final String responseText = response.body() == null ? "<No Response Body>" : response.body().string();
                    throw new IOException("Failed to fetch extension " + bundleCoordinate + " from " + url + ". Received repsonse of " + response.code() + ": " + responseText);
                }
            } finally {
                response.close();

                // Client is no longer needed. We don't have to release the resources from the HTTP Client. They will be
                // cleaned up automatically once the connections time out. However, in this case, since we could not fetch
                // the resources needed, the caller may want to fail aggressively. As such, the HTTP Client should free its
                // resources aggressively in order to avoid holding onto non-daemon threads.
                okHttpClient.dispatcher().executorService().shutdown();
                okHttpClient.connectionPool().evictAll();
            }
        }
    }

    private OkHttpClient createClient() {
        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
            .readTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
            .connectTimeout(timeoutMillis, TimeUnit.MILLISECONDS);

        if (sslContextDefinition != null) {
            final TlsConfiguration tlsConfiguration = SslConfigurationUtil.createTlsConfiguration(sslContextDefinition);
            try {
                final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
                final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
                okHttpClientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
            } catch (final TlsException e) {
                throw new IllegalArgumentException("TLS Configuration Failed: Check SSL Context Properties", e);
            }
        }

        return okHttpClientBuilder.build();
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
