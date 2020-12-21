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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
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

    /**
     * Build a set of allowable TLS/SSL protocol algorithms based on JVM configuration.
     *
     * @return the computed set of allowable values
     */
    static AllowableValue[] buildAlgorithmAllowableValues() {
        final Set<String> supportedProtocols = new HashSet<>();

        /*
         * Prepopulate protocols with generic instance types commonly used
         * see: http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
         */
        supportedProtocols.add(TlsConfiguration.TLS_PROTOCOL);

        // This is still available for outgoing connections to legacy services, but can be disabled with jdk.tls.disabledAlgorithms
        supportedProtocols.add(TlsConfiguration.SSL_PROTOCOL);

        // Determine those provided by the JVM on the system
        try {
            supportedProtocols.addAll(Arrays.asList(SSLContext.getDefault().createSSLEngine().getSupportedProtocols()));
        } catch (NoSuchAlgorithmException e) {
            // ignored as default is used
        }

        return formAllowableValues(supportedProtocols);
    }

    /**
     * Returns an array of {@link AllowableValue} objects formed from the provided
     * set of Strings. The returned array is sorted for consistency in display order.
     *
     * @param rawValues the set of string values
     * @return an array of AllowableValues
     */
    static AllowableValue[] formAllowableValues(Set<String> rawValues) {
        final int numProtocols = rawValues.size();

        // Sort for consistent presentation in configuration views
        final List<String> valueList = new ArrayList<>(rawValues);
        Collections.sort(valueList);

        final List<AllowableValue> allowableValues = new ArrayList<>();
        for (final String protocol : valueList) {
            allowableValues.add(new AllowableValue(protocol));
        }
        return allowableValues.toArray(new AllowableValue[numProtocols]);
    }
}
