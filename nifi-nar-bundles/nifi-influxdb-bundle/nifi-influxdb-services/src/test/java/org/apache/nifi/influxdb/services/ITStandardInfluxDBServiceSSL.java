/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.services;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.influxdb.InfluxDBIOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.io.IOException;
import java.security.GeneralSecurityException;

import static org.apache.nifi.ssl.StandardSSLContextService.KEYSTORE;
import static org.apache.nifi.ssl.StandardSSLContextService.KEYSTORE_PASSWORD;
import static org.apache.nifi.ssl.StandardSSLContextService.KEYSTORE_TYPE;
import static org.apache.nifi.ssl.StandardSSLContextService.TRUSTSTORE;
import static org.apache.nifi.ssl.StandardSSLContextService.TRUSTSTORE_PASSWORD;
import static org.apache.nifi.ssl.StandardSSLContextService.TRUSTSTORE_TYPE;

public class ITStandardInfluxDBServiceSSL extends AbstractTestStandardInfluxDBService {

    @Before
    public void before() throws Exception {

        setUp(CallsRealMethods::new);

        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_URL, "https://localhost:9086");
    }

    @Test
    public void withConfiguredSSL() throws InitializationException, IOException, GeneralSecurityException {

        final SSLContextService sslContextService = new StandardSSLContextService();
        testRunner.addControllerService("ssl-context", sslContextService);

        testRunner.setProperty(sslContextService, TRUSTSTORE, "src/test/resources/ssl/truststore.jks");
        testRunner.setProperty(sslContextService, TRUSTSTORE_PASSWORD, "changeme");
        testRunner.setProperty(sslContextService, TRUSTSTORE_TYPE, "JKS");

        testRunner.setProperty(sslContextService, KEYSTORE, "src/test/resources/ssl/keystore.jks");
        testRunner.setProperty(sslContextService, KEYSTORE_PASSWORD, "changeme");
        testRunner.setProperty(sslContextService, KEYSTORE_TYPE, "JKS");

        testRunner.enableControllerService(sslContextService);
        testRunner.setProperty(service, InfluxDBService.SSL_CONTEXT_SERVICE, "ssl-context");

        testRunner.enableControllerService(service);

        assertConnectToDatabase();
    }

    @Test
    public void withoutConfiguredSSL() throws IOException, GeneralSecurityException {

        testRunner.enableControllerService(service);

        expectedException.expect(InfluxDBIOException.class);
        expectedException.expectMessage("SSLHandshakeException");

        assertConnectToDatabase();
    }
}

