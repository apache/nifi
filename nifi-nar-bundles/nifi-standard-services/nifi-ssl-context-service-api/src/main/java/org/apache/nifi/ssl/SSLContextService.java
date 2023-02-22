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
     * Create and initialize {@link SSLContext} using configured properties. This method is preferred over deprecated
     * create methods due to not requiring a client authentication policy.
     *
     * @return {@link SSLContext} initialized using configured properties
     */
    SSLContext createContext();

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
