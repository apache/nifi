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
package org.apache.nifi.security.encryption;

import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

/**
 * Initialization context for a {@link PropertyEncryptionProvider}
 */
public interface PropertyEncryptionProviderInitializationContext {
    /**
     * Get the properties configured for the Property Encryption Provider
     *
     * @return Configured properties with the application property prefix removed
     */
    Map<String, String> getProperties();

    /**
     * Get the SSLContext for implementations that communicate with remote services
     *
     * @return SSLContext or empty when not configured
     */
    Optional<SSLContext> getSSLContext();

    /**
     * Get the trust manager for implementations that construct their own client TLS configuration
     *
     * @return X509TrustManager or empty when not configured
     */
    Optional<X509TrustManager> getTrustManager();
}
