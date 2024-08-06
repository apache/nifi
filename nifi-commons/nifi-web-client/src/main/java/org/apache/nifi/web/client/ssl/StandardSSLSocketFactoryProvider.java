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
package org.apache.nifi.web.client.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.util.Objects;

/**
 * Standard implementation of SSLSocketFactory Provider
 */
public class StandardSSLSocketFactoryProvider implements SSLSocketFactoryProvider {
    /**
     * Get SSLSocketFactory defaults to system Trust Manager and allows an empty Key Manager
     *
     * @param tlsContext TLS Context configuration
     * @return SSLSocketFactory
     */
    @Override
    public SSLSocketFactory getSocketFactory(final TlsContext tlsContext) {
        Objects.requireNonNull(tlsContext, "TLS Context required");
        final SSLContextProvider sslContextProvider = new StandardSSLContextProvider();
        final SSLContext sslContext = sslContextProvider.getSslContext(tlsContext);
        return sslContext.getSocketFactory();
    }
}
