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

/**
 * Definition for SSLContextService.
 *
 */
@Tags({"ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Provides the ability to configure keystore and/or truststore properties once and reuse "
        + "that configuration throughout the application")
public interface SSLContextService extends ControllerService {

    public static enum ClientAuth {

        WANT,
        REQUIRED,
        NONE
    }

    public SSLContext createSSLContext(final ClientAuth clientAuth) throws ProcessException;

    public String getTrustStoreFile();

    public String getTrustStoreType();

    public String getTrustStorePassword();

    public boolean isTrustStoreConfigured();

    public String getKeyStoreFile();

    public String getKeyStoreType();

    public String getKeyStorePassword();

    public String getKeyPassword();

    public boolean isKeyStoreConfigured();

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
        supportedProtocols.add("TLS");
        supportedProtocols.add("SSL");

        // Determine those provided by the JVM on the system
        try {
            supportedProtocols.addAll(Arrays.asList(SSLContext.getDefault().createSSLEngine().getSupportedProtocols()));
        } catch (NoSuchAlgorithmException e) {
            // ignored as default is used
        }

        final int numProtocols = supportedProtocols.size();

        // Sort for consistent presentation in configuration views
        final List<String> supportedProtocolList = new ArrayList<>(supportedProtocols);
        Collections.sort(supportedProtocolList);

        final List<AllowableValue> protocolAllowableValues = new ArrayList<>();
        for (final String protocol : supportedProtocolList) {
            protocolAllowableValues.add(new AllowableValue(protocol));
        }
        return protocolAllowableValues.toArray(new AllowableValue[numProtocols]);
    }
}
