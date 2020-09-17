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
package org.apache.nifi.processors.solr;

import javax.net.ssl.SSLContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Mock implementation (copied from unit and integration tests) so we don't need to have a real keystore/truststore available for testing.
 *
 * // TODO: Remove and use regular mocking or Groovy rather than shell implementation
 */
public class MockSSLContextService extends AbstractControllerService implements SSLContextService {

    @Override
    public TlsConfiguration createTlsConfiguration() {
        return null;
    }

    @Override
    public SSLContext createSSLContext(org.apache.nifi.security.util.ClientAuth clientAuth) throws ProcessException {
        return null;
    }

    @Override
    public SSLContext createSSLContext(SSLContextService.ClientAuth clientAuth) throws ProcessException {
        return null;
    }

    @Override
    public String getTrustStoreFile() {
        return null;
    }

    @Override
    public String getTrustStoreType() {
        return null;
    }

    @Override
    public String getTrustStorePassword() {
        return null;
    }

    @Override
    public boolean isTrustStoreConfigured() {
        return false;
    }

    @Override
    public String getKeyStoreFile() {
        return null;
    }

    @Override
    public String getKeyStoreType() {
        return null;
    }

    @Override
    public String getKeyStorePassword() {
        return null;
    }

    @Override
    public String getKeyPassword() {
        return null;
    }

    @Override
    public boolean isKeyStoreConfigured() {
        return false;
    }

    @Override
    public String getSslAlgorithm() {
        return null;
    }
}