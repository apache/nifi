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


import org.apache.commons.net.whois.WhoisClient;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WhoisClient.class})
public class TestQueryWhois {
    private QueryWhois queryWhois;
    private TestRunner queryWhoisTestRunner;

    @Before
    public void setupTest() throws Exception {
        // This is what is sent by Mockito
        String header = "AS      | IP               | BGP Prefix          | CC | Registry | Allocated  | Info                    | AS Name\n";
        String responseBodyLine1 =  "999 | 123.123.123.123 | 123.123.123.123/32 | AU | apnic | 2014-01-01 | 2016-08-14 01:32:01 GMT | Apache NiFi\n";
        String responseBodyLine2 =  "333 | 124.124.124.124 | 124.124.124.124/32 | AU | apnic | 2014-01-01 | 2016-08-14 01:32:01 GMT | Apache NiFi\n";

        WhoisClient whoisClient = PowerMockito.mock(WhoisClient.class);
        Mockito.when(whoisClient.query(Mockito.anyString())).thenReturn(header + responseBodyLine1 + responseBodyLine2);

        this.queryWhois =  new QueryWhois() {
            @Override
            protected WhoisClient createClient(){
                return whoisClient;
            }
        };
        this.queryWhoisTestRunner = TestRunners.newTestRunner(queryWhois);

    }



    @Test
    public void testCustomValidator() {
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_SERVER, "127.0.0.1");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_QUERY_TYPE, "peer");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_TIMEOUT, "1000 ms");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_INPUT, "nifi.apache.org");

        // Note the absence of a QUERY_PARSER_INPUT value
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.assertNotValid();


        // Note the presence of a QUERY_PARSER_INPUT value
        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "1");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\|");
        queryWhoisTestRunner.assertValid();

        // Note BULK_PROTOCOL and BATCH_SIZE
        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "1");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\|");
        queryWhoisTestRunner.setProperty(QueryWhois.BULK_PROTOCOL, QueryWhois.BEGIN_END.getValue());
        queryWhoisTestRunner.assertNotValid();

        // Note the presence of a QUERY_PARSER_INPUT value while NONE is set
        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "1");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.NONE.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\|");
        queryWhoisTestRunner.assertNotValid();
//
        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "10");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.NONE.getValue());
        queryWhoisTestRunner.removeProperty(QueryWhois.QUERY_PARSER_INPUT);
        queryWhoisTestRunner.assertNotValid();

        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "10");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\|");
        queryWhoisTestRunner.removeProperty(QueryWhois.KEY_GROUP);
        queryWhoisTestRunner.assertNotValid();

        queryWhoisTestRunner.setProperty(QueryWhois.BATCH_SIZE, "10");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\|");
        queryWhoisTestRunner.removeProperty(QueryWhois.KEY_GROUP);
        queryWhoisTestRunner.assertNotValid();

        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.NONE.getValue());
        queryWhoisTestRunner.assertNotValid();


    }


    @Test
    public void testValidDataWithSplit()  {
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_SERVER, "127.0.0.1");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_QUERY_TYPE, "origin");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_TIMEOUT, "1000 ms");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.SPLIT.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\\s+\\|\\s+");
        queryWhoisTestRunner.setProperty(QueryWhois.KEY_GROUP, "2");

        final Map<String, String> attributeMap1 = new HashMap<>();
        final Map<String, String> attributeMap2 = new HashMap<>();
        final Map<String, String> attributeMap3 = new HashMap<>();
        attributeMap1.put("ip_address", "123.123.123.123");
        attributeMap2.put("ip_address", "124.124.124.124");
        attributeMap3.put("ip_address", "125.125.125.125");

        queryWhoisTestRunner.enqueue(new byte[0], attributeMap1);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap2);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap3);
        queryWhoisTestRunner.run();

        List<MockFlowFile> matchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_FOUND);
        assertTrue(matchingResults.size() == 2);
        List<MockFlowFile> nonMatchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_NOT_FOUND);
        assertTrue(nonMatchingResults.size() == 1);

        matchingResults.get(0).assertAttributeEquals("enrich.whois.record0.group7", "Apache NiFi");
    }

    @Test
    public void testValidDataWithRegex()  {

        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_SERVER, "127.0.0.1");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_QUERY_TYPE, "origin");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_TIMEOUT, "1000 ms");
        queryWhoisTestRunner.setProperty(QueryWhois.KEY_GROUP, "2");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\n^([^\\|]*)\\|\\s+(\\S+)\\s+\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|(.*)$");

        final Map<String, String> attributeMap1 = new HashMap<>();
        final Map<String, String> attributeMap2 = new HashMap<>();
        final Map<String, String> attributeMap3 = new HashMap<>();
        attributeMap1.put("ip_address", "123.123.123.123");
        attributeMap2.put("ip_address", "124.124.124.124");
        attributeMap3.put("ip_address", "125.125.125.125");

        queryWhoisTestRunner.enqueue(new byte[0], attributeMap1);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap2);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap3);
        queryWhoisTestRunner.run();

        List<MockFlowFile> matchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_FOUND);
        assertTrue(matchingResults.size() == 2);
        List<MockFlowFile> nonMatchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_NOT_FOUND);
        assertTrue(nonMatchingResults.size() == 1);

        matchingResults.get(0).assertAttributeEquals("enrich.whois.record0.group8", " Apache NiFi");

    }

    @Test
    public void testValidDataWithRegexButInvalidCaptureGroup()  {

        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_SERVER, "127.0.0.1");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_QUERY_TYPE, "origin");
        queryWhoisTestRunner.setProperty(QueryWhois.WHOIS_TIMEOUT, "1000 ms");
        queryWhoisTestRunner.setProperty(QueryWhois.KEY_GROUP, "9");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_INPUT, "${ip_address:getDelimitedField(4, '.'):trim()}" +
                ".${ip_address:getDelimitedField(3, '.'):trim()}" +
                ".${ip_address:getDelimitedField(2, '.'):trim()}" +
                ".${ip_address:getDelimitedField(1, '.'):trim()}");
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER, QueryWhois.REGEX.getValue());
        queryWhoisTestRunner.setProperty(QueryWhois.QUERY_PARSER_INPUT, "\n^([^\\|]*)\\|\\s+(\\S+)\\s+\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|([^\\|]*)\\|(.*)$");

        final Map<String, String> attributeMap1 = new HashMap<>();
        final Map<String, String> attributeMap2 = new HashMap<>();
        final Map<String, String> attributeMap3 = new HashMap<>();
        attributeMap1.put("ip_address", "123.123.123.123");
        attributeMap2.put("ip_address", "124.124.124.124");
        attributeMap3.put("ip_address", "125.125.125.125");

        queryWhoisTestRunner.enqueue(new byte[0], attributeMap1);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap2);
        queryWhoisTestRunner.enqueue(new byte[0], attributeMap3);
        queryWhoisTestRunner.run();

        List<MockFlowFile> matchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_FOUND);
        assertTrue(matchingResults.size() == 0);
        List<MockFlowFile> nonMatchingResults = queryWhoisTestRunner.getFlowFilesForRelationship(QueryWhois.REL_NOT_FOUND);
        assertTrue(nonMatchingResults.size() == 3);

    }

}

