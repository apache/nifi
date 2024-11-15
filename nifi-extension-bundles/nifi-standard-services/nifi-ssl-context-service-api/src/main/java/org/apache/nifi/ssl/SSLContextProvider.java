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

import org.apache.nifi.controller.ControllerService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import java.util.Optional;

/**
 * Controller Service abstraction for creating instances of javax.net.ssl.SSLContext without access to supporting configuration properties
 */
public interface SSLContextProvider extends ControllerService {
    /**
     * Create and initialize an SSLContext with keys and certificates based on configuration
     *
     * @return SSLContext initialized using configured properties
     */
    SSLContext createContext();

    /**
     * Create and initialize an X.509 Key Manager when configured with key and certificate properties
     *
     * @return X.509 Extended Key Manager or empty when not configured
     */
    Optional<X509ExtendedKeyManager> createKeyManager();

    /**
     * Create and initialize an X.509 Trust Manager with certificates
     *
     * @return X509ExtendedTrustManager initialized using configured certificates
     */
    X509TrustManager createTrustManager();
}
