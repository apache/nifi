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
import org.influxdb.InfluxDB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.GeneralSecurityException;

import static org.mockito.Mockito.when;

public class TestStandardInfluxDBServiceSettings extends AbstractTestStandardInfluxDBService {

    @Before
    public void before() throws Exception {

        setUp(() -> invocation -> Mockito.mock(InfluxDB.class));
    }

    @Test
    public void defaultSettingsIsValid() {

        testRunner.assertValid(service);
    }

    @Test
    public void defaultSettings() throws IOException, GeneralSecurityException {

        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void sslContextService() throws InitializationException, IOException, GeneralSecurityException {

        SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn("inluxdb-ssl");
        testRunner.addControllerService("inluxdb-ssl", sslContextService);
        testRunner.enableControllerService(sslContextService);

        testRunner.setProperty(service, InfluxDBService.SSL_CONTEXT_SERVICE, "inluxdb-ssl");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(sslContextService),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void clientAuth() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDBService.CLIENT_AUTH, SSLContextService.ClientAuth.NONE.name());
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(SSLContextService.ClientAuth.NONE),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void url() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_URL, "http://influxdb:8886");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://influxdb:8886"),
                Mockito.eq(0L));
    }

    @Test
    public void urlValidation() {

        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_URL, "not_url");
        testRunner.assertNotValid(service);

        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_URL, "");
        testRunner.assertNotValid(service);
    }

    @Test
    public void dbConnectionTimeout() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_CONNECTION_TIMEOUT, "100 mins");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(6000L));
    }

    @Test
    public void dbConnectionTimeoutValidation() {

        // not number value
        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_CONNECTION_TIMEOUT, "text");
        testRunner.assertNotValid(service);

        // without unit
        testRunner.setProperty(service, InfluxDBService.INFLUX_DB_CONNECTION_TIMEOUT, "100");
        testRunner.assertNotValid(service);
    }

    @Test
    public void username() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDBService.USERNAME, "user-name");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("user-name"),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void usernameValidation() {

        testRunner.setProperty(service, InfluxDBService.USERNAME, "");
        testRunner.assertNotValid(service);

        testRunner.setProperty(service, InfluxDBService.USERNAME, " ");
        testRunner.assertNotValid(service);
    }

    @Test
    public void password() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDBService.PASSWORD, "pass-word");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.connect();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq(null),
                Mockito.eq("pass-word"),
                Mockito.eq(null),
                Mockito.eq(InfluxDBService.DEFAULT_CLIENT_AUTH),
                Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void passwordValidation() {

        testRunner.setProperty(service, InfluxDBService.PASSWORD, "");
        testRunner.assertNotValid(service);

        testRunner.setProperty(service, InfluxDBService.PASSWORD, " ");
        testRunner.assertNotValid(service);
    }
}
