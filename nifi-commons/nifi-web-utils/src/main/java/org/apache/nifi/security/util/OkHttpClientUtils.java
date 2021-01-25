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
package org.apache.nifi.security.util;

import java.security.UnrecoverableKeyException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains utility methods for working with the {@link OkHttpClient} that many components use for external HTTP communication.
 */
public class OkHttpClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(OkHttpClientUtils.class);

    /**
     * If the {@link TlsConfiguration} contains valid properties to configure an
     * {@link SSLSocketFactory}, parses the necessary values and applies the config to the client
     * builder. If the properties are not populated, no action is taken.
     *
     * @param tlsConfiguration the TLS configuration container object
     * @param okHttpClient     the OkHttp client builder
     * @return true if the TLS configuration was applied to the builder
     */
    public static boolean applyTlsToOkHttpClientBuilder(TlsConfiguration tlsConfiguration, OkHttpClient.Builder okHttpClient) {
        try {
            final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
            if (trustManager == null) {
                return false;
            }

            final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration, new TrustManager[]{trustManager});
            if (sslContext == null) {
                return false;
            }

            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            okHttpClient.sslSocketFactory(sslSocketFactory, trustManager);
            return true;
        } catch (TlsException e) {
            if (e.getCause() instanceof UnrecoverableKeyException) {
                logger.error("Key password may be incorrect or not set. Check your keystore passwords." + e.getMessage());
            } else {
                logger.error("Encountered an error configuring TLS: {}", e.getLocalizedMessage());
                throw new ProcessException("Error configuring TLS", e);
            }
        }
        return false;
    }
}
