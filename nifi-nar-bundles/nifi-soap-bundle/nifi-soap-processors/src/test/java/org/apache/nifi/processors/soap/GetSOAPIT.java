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
package org.apache.nifi.processors.soap;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.nifi.processors.soap.GetSOAP.REL_SUCCESS;

/**
 * Integration test that will query live soap services.
 */
public class GetSOAPIT {

    private static final Logger logger = LoggerFactory.getLogger(GetSOAPIT.class);

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetSOAP.class);
    }

    @After
    public void after() {
        testRunner.shutdown();
    }

    /**
     * Public service that does not require authentication.
     */
    @Test
    public void testWeather() {
        testRunner.setProperty(GetSOAP.ENDPOINT_URL,
                               "https://graphical.weather.gov:443/xml/SOAP_server/ndfdXMLserver.php");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://graphical.weather.gov/xml/DWMLgen/wsdl/ndfdXML.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "LatLonListZipCode");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://graphical.weather.gov/xml/DWMLgen/wsdl/ndfdXML.wsdl");
        testRunner.setProperty(GetSOAP.SKIP_FIRST_ELEMENT, "true");
        testRunner.setProperty("zipCodeList", "90720");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);
        for (MockFlowFile flowFile : flowFileList) {
            String data = new String(testRunner.getContentAsByteArray(flowFile));
            logger.info("Data returned: {}", data);
        }
    }

    /**
     * Public service that does not require authentication. This service required the SOAPAction header to be set.
     * The others didn't seem to require it.
     */
    //Using Ignore as we do not want this to run on a build server.
    @Ignore
    @Test
    public void testDilbert() {
        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://gcomputer.net/webservices/dilbert.asmx");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://www.gcomputer.net/webservices/dilbert.asmx?WSDL");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "DailyDilbert");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://gcomputer.net/webservices/");
        testRunner.setProperty("ADate", "2017-02-01T00:00:00");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);
        for (MockFlowFile flowFile : flowFileList) {
            String data = new String(testRunner.getContentAsByteArray(flowFile));
            logger.info("Data returned: {}", data);
        }
    }

    /**
     * Public service that does not require authentication.
     */
    //Using Ignore as we do not want this to run on a build server.
    //Also FYI -- often-times even when using the test page for this WS found at the ENDPOINT_URL
    //it returns an exception.
    @Ignore
    @Test
    public void testStockQuote() {
        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://www.webservicex.com/stockquote.asmx");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://www.webservicex.com/stockquote.asmx?WSDL");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "GetQuote");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://www.webserviceX.NET/");
        testRunner.setProperty(GetSOAP.SKIP_FIRST_ELEMENT, "true");
        testRunner.setProperty("symbol", "YHOO"); // Maybe delisted soon? lol

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);
        for (MockFlowFile flowFile : flowFileList) {
            String data = new String(testRunner.getContentAsByteArray(flowFile));
            logger.debug("Data returned: {}", data);
            Assert.assertTrue(data.startsWith("<StockQuotes><Stock><Symbol>YHOO</Symbol>"));
        }
    }

    /**
     * This is a test of a private service that does require authentication. In order to run this test,
     * you will need to fill in the endpoint, wsdl, username, password, etc.
     */
    //Using Ignore as we do not want this to run on a build server.
    @Ignore
    @Test
    public void testServiceWithAuthentication() {
        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://api.someservice.com/v3/SomeService");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://api.someservice.com/v3/SomeService?wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "getTransactionList");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://api.someservice.com/");
        // Testing auth by header and passing username/password as variables.
        testRunner.setProperty(GetSOAP.AUTH_BY_HEADER, "{\"UserAuthentication\": { " +
                                                       " \"iId\": \"${Username}\", " +
                                                       " \"sPassword\": \"${Password}\", " +
                                                       " \"sType\": \"merchant\" " +
                                                       "  }" +
                                                       "}");
        // Username/Password must be set if referenced above
        testRunner.setProperty(GetSOAP.USER_NAME, "PUT_YOUR_USER_NAME_HERE");
        testRunner.setProperty(GetSOAP.PASSWORD, "PUT_YOUR_PASSWORD_HERE");
        testRunner.setProperty("sDateType", "transaction");
        // Make sure that dynamic properties can evaluate expression language
        testRunner.setProperty("dStartDate", "${now():toNumber():minus(86400000):format(\"yyyy-MM-dd'T'HH:mm:ss\")}");
        testRunner.setProperty("dEndDate", "${now():format(\"yyyy-MM-dd'T'HH:mm:ss\")}");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetSOAP.REL_SUCCESS);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(GetSOAP.REL_SUCCESS);
        Assert.assertNotNull(flowFileList);
        for (MockFlowFile flowFile : flowFileList) {
            String data = new String(testRunner.getContentAsByteArray(flowFile));
            logger.info("DATA" + data);
        }
    }
}
