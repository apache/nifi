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
package org.apache.nifi.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.TlsConfiguration;

/**
 * Definition for SSLContextService.
 *
 */
@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Provides the ability to configure keystore and/or truststore properties once and reuse "
        + "that configuration throughout the application")
public interface SSLContextService extends ControllerService {

    TlsConfiguration createTlsConfiguration();

    /**
     * This enum was removed in 1.12.0 but external custom code has been compiled against it, so it is returned
     * in 1.12.1. This enum should no longer be used and any dependent code should now reference
     * ClientAuth moving forward. This enum may be removed in a future release.
     *
     */
    @Deprecated
    enum ClientAuth {
        WANT,
        REQUIRED,
        NONE
    }

    /**
     * Create and initialize {@link SSLContext} using configured properties. This method is preferred over deprecated
     * create methods due to not requiring a client authentication policy.
     *
     * @return {@link SSLContext} initialized using configured properties
     */
    SSLContext createContext();

    /**
     * Returns a configured {@link SSLContext} from the populated configuration values. This method is deprecated
     * due to {@link org.apache.nifi.security.util.ClientAuth} not being applicable or used when initializing the
     * {@link SSLContext}
     *
     * @param clientAuth the desired level of client authentication
     * @return the configured SSLContext
     * @throws ProcessException if there is a problem configuring the context
     * @deprecated The {@link #createContext()} method should be used instead
     */
    @Deprecated
    SSLContext createSSLContext(org.apache.nifi.security.util.ClientAuth clientAuth) throws ProcessException;

    /**
     * Returns a configured {@link SSLContext} from the populated configuration values. This method is deprecated
     * due to the use of the deprecated {@link ClientAuth} enum and the
     * ({@link #createContext()}) method is preferred.
     *
     * @param clientAuth the desired level of client authentication
     * @return the configured SSLContext
     * @throws ProcessException if there is a problem configuring the context
     * @deprecated The {@link #createContext()} method should be used instead
     */
    @Deprecated
    SSLContext createSSLContext(ClientAuth clientAuth) throws ProcessException;

    /**
     * Create X.509 Trust Manager using configured properties
     *
     * @return {@link X509TrustManager} initialized using configured properties
     */
    X509TrustManager createTrustManager();

    String getTrustStoreFile();

    String getTrustStoreType();

    String getTrustStorePassword();

    boolean isTrustStoreConfigured();

    String getKeyStoreFile();

    String getKeyStoreType();

    String getKeyStorePassword();

    String getKeyPassword();

    boolean isKeyStoreConfigured();

    String getSslAlgorithm();
}
