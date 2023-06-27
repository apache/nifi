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
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * <p>The {@link KeyStoreScanner} is used to monitor the KeyStore file used by the {@link SslContextFactory}.
 * It will recreate the {@code SSLContext} and reload the {@link SslContextFactory} if it detects that the KeyStore
 * file has been modified.</p>
 */
public class KeyStoreScanner extends AbstractStoreScanner {
    private static final String FILE_TYPE = "keystore";
    private static final Logger LOG = Log.getLogger(KeyStoreScanner.class);

    public KeyStoreScanner(final SslContextFactory sslContextFactory,
                           final TlsConfiguration tlsConfiguration) {
        super(sslContextFactory, tlsConfiguration);
    }

    @Override
    protected String getFileType() {
        return FILE_TYPE;
    }

    @Override
    protected Resource getResource() {
        return getSslContextFactory().getKeyStoreResource();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
