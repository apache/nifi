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
package org.apache.nifi.web.server.util;

import org.apache.nifi.security.util.TlsConfiguration;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * <p>The {@link TrustStoreScanner} is used to monitor the TrustStore file used by the {@link SslContextFactory}.
 * It will reload the {@link SslContextFactory} if it detects that the TrustStore file has been modified.</p>
 * <p>
 * Though it would have been more ideal to simply extend KeyStoreScanner and override the keystore resource
 * with the truststore resource, KeyStoreScanner's constructor was written in a way that doesn't make this possible.
 */
public class TrustStoreScanner extends StoreScanner {
    private static final String FILE_TYPE = "truststore";

    public TrustStoreScanner(final SslContextFactory sslContextFactory,
                             final TlsConfiguration tlsConfiguration) {
        super(sslContextFactory, tlsConfiguration, sslContextFactory.getTrustStoreResource(), FILE_TYPE);
    }
}