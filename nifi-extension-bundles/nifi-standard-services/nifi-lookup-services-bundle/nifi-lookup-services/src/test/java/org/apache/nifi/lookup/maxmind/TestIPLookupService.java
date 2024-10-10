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

package org.apache.nifi.lookup.maxmind;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.nifi.lookup.TestProcessor;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class TestIPLookupService {
    private TestRunner runner;
    private IPLookupService testSubject;

    private DatabaseReader mockDatabaseReader;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        testSubject = new IPLookupService() {
            @NotNull
            @Override
            DatabaseReader createDatabaseReader(File dbFile) throws IOException {
                return mockDatabaseReader;
            }
        };

        mockDatabaseReader = Mockito.mock(DatabaseReader.class);
    }

    @Test
    void testLookupDefaultNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryCity(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryCity(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }

    @Test
    void testLookupCityNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CITY, "true");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryCity(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryCity(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }

    @Test
    void testLookupISPNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_ISP, "true");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryIsp(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryIsp(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }

    @Test
    void testLookupDomainNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_DOMAIN, "true");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryDomain(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryDomain(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }

    @Test
    void testLookupConnectionTypeNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CONNECTION_TYPE, "true");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryConnectionType(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryConnectionType(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }

    @Test
    void testLookupAnonymousIpNoResult() throws Exception {
        // GIVEN
        runner.addControllerService("testSubject", testSubject);
        runner.setProperty(testSubject, IPLookupService.GEO_DATABASE_FILE, "src/test/resources/dummy.mmdb");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_CITY, "false");
        runner.setProperty(testSubject, IPLookupService.LOOKUP_ANONYMOUS_IP_INFO, "true");
        runner.enableControllerService(testSubject);
        runner.assertValid(testSubject);

        final IPLookupService lookupService = (IPLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("testSubject");

        Mockito.when(mockDatabaseReader.tryAnonymousIp(any(InetAddress.class))).thenReturn(Optional.empty());

        // WHEN
        final Optional<Record> lookupResult = lookupService.lookup(Collections.singletonMap(IPLookupService.IP_KEY, "0.0.0.0"));

        // THEN
        Mockito.verify(mockDatabaseReader).tryAnonymousIp(any(InetAddress.class));

        assertEquals(Optional.empty(), lookupResult);
    }
}
