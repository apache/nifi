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
package org.apache.nifi.framework.ssl;

import org.apache.nifi.security.ssl.KeyManagerBuilder;
import org.apache.nifi.security.ssl.TrustManagerBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.util.Objects;

/**
 * Holder class for sharing SSLContext components loaded in Jetty Server with Spring Application Context objects
 */
public class FrameworkSslContextHolder {
    private static SSLContext sslContext;

    private static X509ExtendedKeyManager keyManager;

    private static X509ExtendedTrustManager trustManager;

    private static KeyManagerBuilder keyManagerBuilder;

    private static TrustManagerBuilder trustManagerBuilder;

    public static void setSslContext(final SSLContext sslContext) {
        FrameworkSslContextHolder.sslContext = Objects.requireNonNull(sslContext, "SSL Context required");
    }

    public static SSLContext getSslContext() {
        return sslContext;
    }

    public static void setKeyManager(final X509ExtendedKeyManager keyManager) {
        FrameworkSslContextHolder.keyManager = Objects.requireNonNull(keyManager, "Key Manager required");
    }

    public static X509ExtendedKeyManager getKeyManager() {
        return keyManager;
    }

    public static void setKeyManagerBuilder(final KeyManagerBuilder keyManagerBuilder) {
        FrameworkSslContextHolder.keyManagerBuilder = Objects.requireNonNull(keyManagerBuilder, "Key Manager Builder required");
    }

    public static KeyManagerBuilder getKeyManagerBuilder() {
        return keyManagerBuilder;
    }

    public static void setTrustManager(final X509ExtendedTrustManager trustManager) {
        FrameworkSslContextHolder.trustManager = Objects.requireNonNull(trustManager, "Trust Manager required");
    }

    public static X509ExtendedTrustManager getTrustManager() {
        return trustManager;
    }

    public static void setTrustManagerBuilder(final TrustManagerBuilder trustManagerBuilder) {
        FrameworkSslContextHolder.trustManagerBuilder = Objects.requireNonNull(trustManagerBuilder, "Trust Manager Builder required");
    }

    public static TrustManagerBuilder getTrustManagerBuilder() {
        return trustManagerBuilder;
    }
}
