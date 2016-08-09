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

package org.apache.nifi.processors.enrich;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


import javax.naming.Context;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;

import static org.junit.Assert.assertTrue;

public class TestQueryDNS  {
    private QueryDNS queryDNS;
    private TestRunner queryDNSTestRunner;

    @Before
    public void setupTest() throws Exception {
        this.queryDNS =  new QueryDNS();
        this.queryDNSTestRunner = TestRunners.newTestRunner(queryDNS);

        Hashtable env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, FakeDNSInitialDirContextFactory.class.getName());

        this.queryDNS.initializeContext(env);

        final DirContext mockContext = FakeDNSInitialDirContextFactory.getLatestMockContext();

        // Capture JNDI's getAttibutes method containing the (String) queryValue and (String[]) queryType
        Mockito.when( mockContext.getAttributes(Mockito.anyString(), Mockito.any(String[].class)))
                .thenAnswer(new Answer() {
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        // Craft a false DNS response
                        // Note the DNS response will not make use of any of the mocked
                        // query contents (all input is discarded and replies synthetically
                        // generated
                        return craftResponse(invocation);
                    }
                });
    }

    @Test
    public void testVanillaQueryWithoutSplit()  {
        queryDNSTestRunner.setProperty(QueryDNS.DNS_QUERY_TYPE, "PTR");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_RETRIES, "1");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_TIMEOUT, "1000 ms");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}" +
                ".in-addr.arpa");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.NONE.getValue());

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("ip_address", "123.123.123.123");

        queryDNSTestRunner.enqueue(new byte[0], attributeMap);
        queryDNSTestRunner.enqueue("teste teste teste chocolate".getBytes());

        queryDNSTestRunner.run(1,true, false);

        List<MockFlowFile> results = queryDNSTestRunner.getFlowFilesForRelationship(QueryDNS.REL_FOUND);
        assertTrue(results.size() == 1);
        String result = results.get(0).getAttribute("enrich.dns.record0.group0");

        assertTrue(result.contains("apache.nifi.org"));


    }

    @Test
    public void testValidDataWithSplit()  {
        queryDNSTestRunner.setProperty(QueryDNS.DNS_QUERY_TYPE, "TXT");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_RETRIES, "1");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_TIMEOUT, "1000 ms");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}" +
                ".origin.asn.nifi.apache.org");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.SPLIT.getValue());
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER_INPUT, "\\|");

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("ip_address", "123.123.123.123");

        queryDNSTestRunner.enqueue(new byte[0], attributeMap);
        queryDNSTestRunner.enqueue("teste teste teste chocolate".getBytes());
        queryDNSTestRunner.run(1,true, false);

        List<MockFlowFile> results = queryDNSTestRunner.getFlowFilesForRelationship(QueryDNS.REL_FOUND);
        assertTrue(results.size() == 1);

        results.get(0).assertAttributeEquals("enrich.dns.record0.group5", " Apache NiFi");
    }

    @Test
    public void testValidDataWithRegex()  {

        queryDNSTestRunner.setProperty(QueryDNS.DNS_QUERY_TYPE, "TXT");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_RETRIES, "1");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_TIMEOUT, "1000 ms");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}" +
                ".origin.asn.nifi.apache.org");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.REGEX.getValue());
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER_INPUT, "\\.*(\\sApache\\sNiFi)$");

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("ip_address", "123.123.123.123");

        queryDNSTestRunner.enqueue(new byte[0], attributeMap);
        queryDNSTestRunner.enqueue("teste teste teste chocolate".getBytes());
        queryDNSTestRunner.run(1, true, false);

        List<MockFlowFile> results = queryDNSTestRunner.getFlowFilesForRelationship(QueryDNS.REL_FOUND);
        assertTrue(results.size() == 1);

        results.get(0).assertAttributeEquals("enrich.dns.record0.group0", " Apache NiFi");

    }

    @Test
    public void testInvalidData()  {
        queryDNSTestRunner.setProperty(QueryDNS.DNS_QUERY_TYPE, "AAAA");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_RETRIES, "1");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_TIMEOUT, "1000 ms");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_INPUT, "nifi.apache.org");


        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("ip_address", "123.123.123.123");

        queryDNSTestRunner.enqueue(new byte[0], attributeMap);
        queryDNSTestRunner.enqueue("teste teste teste chocolate".getBytes());
        queryDNSTestRunner.run(1, true, false);

        List<MockFlowFile> results = queryDNSTestRunner.getFlowFilesForRelationship(QueryDNS.REL_NOT_FOUND);
        assertTrue(results.size() == 1);
    }

    @Test
    public void testCustomValidator() {
        queryDNSTestRunner.setProperty(QueryDNS.DNS_QUERY_TYPE, "AAAA");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_RETRIES, "1");
        queryDNSTestRunner.setProperty(QueryDNS.DNS_TIMEOUT, "1000 ms");
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_INPUT, "nifi.apache.org");
        // Note the absence of a QUERY_PARSER_INPUT value
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.REGEX.getValue());
        queryDNSTestRunner.assertNotValid();

        // Note the presence of a QUERY_PARSER_INPUT value
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.REGEX.getValue());
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER_INPUT, "\\|");
        queryDNSTestRunner.assertValid();

        // Note the presence of a QUERY_PARSER_INPUT value while NONE is set
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER, QueryDNS.NONE.getValue());
        queryDNSTestRunner.setProperty(QueryDNS.QUERY_PARSER_INPUT, "\\|");
        queryDNSTestRunner.assertNotValid();
    }




    // Dummy pseudo-DNS responder
    private Attributes craftResponse(InvocationOnMock invocation) {
        Object[] arguments = invocation.getArguments();
        String[] queryType = (String[]) arguments[1];

        // Create attribute
        Attributes attrs = new BasicAttributes(true);
        BasicAttribute attr;

        switch (queryType[0]) {
            case "AAAA":
                attr = new BasicAttribute("AAAA");
                attrs.put(attr);
                break;
            case "TXT":
                attr =  new BasicAttribute("TXT", "666 | 123.123.123.123/32 | Apache-NIFI | AU | nifi.org | Apache NiFi");
                attrs.put(attr);
                break;
            case "PTR":
                attr = new BasicAttribute("PTR");
                attr.add(0, "eg-apache.nifi.org.");
                attr.add(1, "apache.nifi.org.");
                attrs.put(attr);
                break;
        }
        return attrs;
    }
}

