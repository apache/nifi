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
package org.apache.nifi.processors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip2.model.IspResponse;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ISPEnrichIP.class})
@SuppressWarnings("WeakerAccess")
public class TestISPEnrichIP {
    DatabaseReader databaseReader;
    ISPEnrichIP ispEnrichIP;
    TestRunner testRunner;

    @Before
    public void setUp() throws Exception {
        mockStatic(InetAddress.class);
        databaseReader = mock(DatabaseReader.class);
        ispEnrichIP = new TestableIspEnrichIP();
        testRunner = TestRunners.newTestRunner(ispEnrichIP);
    }

    @Test
    public void verifyNonExistentIpFlowsToNotFoundRelationship() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        testRunner.enqueue(new byte[0], Collections.emptyMap());

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());

        verify(databaseReader).isp(InetAddress.getByName(null));
    }

    @Test
    public void successfulMaxMindResponseShouldFlowToFoundRelationship() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final IspResponse ispResponse = getIspResponse("1.2.3.4");

        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenReturn(ispResponse);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip.isp.lookup.micros"));
        assertEquals("Apache NiFi - Test ISP", finishedFound.getAttribute("ip.isp.name"));
        assertEquals("Apache NiFi - Test Organization", finishedFound.getAttribute("ip.isp.organization"));
        assertEquals("1337", finishedFound.getAttribute("ip.isp.asn"));
        assertEquals("Apache NiFi - Test Chocolate", finishedFound.getAttribute("ip.isp.asn.organization"));
    }

    @Test
    public void successfulMaxMindResponseShouldFlowToFoundRelationshipWhenAsnIsNotSet() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final IspResponse ispResponse = getIspResponseWithoutASNDetail("1.2.3.4");

        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenReturn(ispResponse);


        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip.isp.lookup.micros"));
        assertNotNull(finishedFound.getAttribute("ip.isp.lookup.micros"));
        assertEquals("Apache NiFi - Test ISP", finishedFound.getAttribute("ip.isp.name"));
        assertEquals("Apache NiFi - Test Organization", finishedFound.getAttribute("ip.isp.organization"));
        assertNull(finishedFound.getAttribute("ip.isp.asn"));
        assertNull(finishedFound.getAttribute("ip.isp.asn.organization"));
    }

    @Test
    public void evaluatingExpressionLanguageShouldAndFindingIpFieldWithSuccessfulLookUpShouldFlowToFoundRelationship() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "${ip.fields:substringBefore(',')}");

        final IspResponse ispResponse = getIspResponse("1.2.3.4");
        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenReturn(ispResponse);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip.fields", "ip0,ip1,ip2");
        attributes.put("ip0", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip0.isp.lookup.micros"));
        assertEquals("Apache NiFi - Test ISP", finishedFound.getAttribute("ip0.isp.name"));
        assertEquals("Apache NiFi - Test Organization", finishedFound.getAttribute("ip0.isp.organization"));
        assertEquals("1337", finishedFound.getAttribute("ip0.isp.asn"));
        assertEquals("Apache NiFi - Test Chocolate", finishedFound.getAttribute("ip0.isp.asn.organization"));

    }

    @Test
    public void shouldFlowToNotFoundWhenNullResponseFromMaxMind() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenReturn(null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFlowToNotFoundWhenIOExceptionThrownFromMaxMind() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");


        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenThrow(IOException.class);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFlowToNotFoundWhenExceptionThrownFromMaxMind() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        when(databaseReader.isp(InetAddress.getByName("1.2.3.4"))).thenThrow(IOException.class);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whenInetAddressThrowsUnknownHostFlowFileShouldBeSentToNotFound() throws Exception {
        testRunner.setProperty(ISPEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(ISPEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "somenonexistentdomain.comm");

        when(InetAddress.getByName("somenonexistentdomain.comm")).thenThrow(UnknownHostException.class);

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(ISPEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());

        verify(databaseReader).close();
        verifyNoMoreInteractions(databaseReader);
    }

    private IspResponse getIspResponse(final String ipAddress) throws Exception {
        final String maxMindIspResponse = "{\n" +
            "         \"isp\" : \"Apache NiFi - Test ISP\",\n" +
            "         \"organization\" : \"Apache NiFi - Test Organization\",\n" +
            "         \"autonomous_system_number\" : 1337,\n" +
            "         \"autonomous_system_organization\" : \"Apache NiFi - Test Chocolate\", \n" +
            "         \"ip_address\" : \"" + ipAddress + "\"\n" +
            "      }\n";

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


        return new ObjectMapper().readerFor(IspResponse.class).with(inject).readValue(maxMindIspResponse);

    }

    private IspResponse getIspResponseWithoutASNDetail(final String ipAddress) throws Exception {
        final String maxMindIspResponse = "{\n" +
            "         \"isp\" : \"Apache NiFi - Test ISP\",\n" +
            "         \"organization\" : \"Apache NiFi - Test Organization\",\n" +
            "         \"autonomous_system_number\" : null,\n" +
            "         \"ip_address\" : \"" + ipAddress + "\"\n" +
            "      }\n";

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


        return new ObjectMapper().readerFor(IspResponse.class).with(inject).readValue(maxMindIspResponse);
    }


    class TestableIspEnrichIP extends ISPEnrichIP {
        @OnScheduled
        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            databaseReaderRef.set(databaseReader);
        }
    }

}
