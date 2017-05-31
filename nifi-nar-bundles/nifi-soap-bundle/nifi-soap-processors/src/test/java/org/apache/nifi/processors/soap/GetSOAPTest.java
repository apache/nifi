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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.impl.common.OMNamespaceImpl;
import org.apache.axiom.om.impl.llom.OMElementImpl;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.soap.GetSOAP.REL_SUCCESS;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class GetSOAPTest {

    private static final Logger logger = LoggerFactory.getLogger(GetSOAPTest.class);

    private TestRunner testRunner;

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void setup() {
        mockServer = startClientAndServer(1080);

        final String xmlBody = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
                               "<SOAP-ENV:Envelope SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">\n" +
                               "    <SOAP-ENV:Body>\n" +
                               "        <ns1:LatLonListZipCodeResponse xmlns:ns1=\"http://graphical.weather.gov/xml/DWMLgen/wsdl/ndfdXML.wsdl\">\n" +
                               "            <listLatLonOut xsi:type=\"xsd:string\">&lt;?xml version=&apos;1.0&apos;?&gt;&lt;dwml version=&apos;1.0&apos; xmlns:xsd=&apos;http://www.w3.org/2001/XMLSchema&apos; xmlns:xsi=&apos;http://www.w3.org/2001/XMLSchema-instance&apos; xsi:noNamespaceSchemaLocation=&apos;http://graphical.weather.gov/xml/DWMLgen/schema/DWML.xsd&apos;&gt;&lt;latLonList&gt;35.9153,-79.0838&lt;/latLonList&gt;&lt;/dwml&gt;</listLatLonOut>\n" +
                               "        </ns1:LatLonListZipCodeResponse>\n" +
                               "    </SOAP-ENV:Body>\n" +
                               "</SOAP-ENV:Envelope>";

        mockServer.when(request().withMethod("POST").withPath("/test_path"))
                  .respond(response().withBody(xmlBody));

        // This callback will verify the request is valid XML
        mockServer.when(request().withMethod("POST").withPath("/test_path_callback"))
                  .callback(HttpCallback.callback()
                                        .withCallbackClass(VerifyRequestCallback.class.getName()));
    }

    @AfterClass
    public static void tearDown() {
        mockServer.stop();
    }

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetSOAP.class);
    }

    @After
    public void after() {
        testRunner.shutdown();
    }

    /**
     * Verify that invalid json for the auth header throws an AssertionError
     */
    @Test(expected = AssertionError.class)
    public void testInvalidHeader() {

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "testMethod");
        testRunner.setProperty(GetSOAP.AUTH_BY_HEADER, "{ invalid_json: \"\"");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");

        testRunner.enqueue("");
        testRunner.run();
    }

    /**
     * This test verifies a method name set using expression language works.
     */
    @Test
    public void testExpressionMethodName() {

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path_callback");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path_callback.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "${method_name}");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("method_name", "getTransactionList");

        // Here we are specifying the method name in an attribute of the flow file. It should succeed.
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidMethodName() {

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path_callback");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path_callback.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "${method_name}");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");

        // Verify that if we don't specify the method name that an error is thrown.
        testRunner.enqueue("");
        testRunner.run();
    }

    @Test
    public void testValidHeaderAuth() {

        testRunner.setProperty(GetSOAP.USER_NAME, "PUT_YOUR_USER_NAME_HERE");
        testRunner.setProperty(GetSOAP.PASSWORD, "PUT_YOUR_PASSWORD_HERE");
        testRunner.setProperty(GetSOAP.AUTH_BY_HEADER, "{\n" +
                                                       "  \"UserAuthentication\": {\n" +
                                                       "    \"iId\": \"${Username}\",\n" +
                                                       "    \"sPassword\": \"${Password}\",\n" +
                                                       "    \"sType\": \"merchant\"\n" +
                                                       "  }\n" +
                                                       "}");

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "testMethod");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);
    }

    @Test
    public void testHTTPUsernamePasswordProcessor() throws IOException {

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "testMethod");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");
        testRunner.setProperty(GetSOAP.SKIP_FIRST_ELEMENT, "true");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);

        final String expectedBody = "<?xml version='1.0'?><dwml version='1.0' xmlns:xsd='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='http://graphical.weather.gov/xml/DWMLgen/schema/DWML.xsd'><latLonList>35.9153,-79.0838</latLonList></dwml>";
        String contents = new String(testRunner.getContentAsByteArray(flowFileList.get(0)));
        logger.debug("Contents: {}", contents);
        flowFileList.get(0).assertContentEquals(expectedBody.getBytes());

    }

    @Test
    public void testHTTPWithUsernamePasswordProcessor() throws IOException {

        testRunner.setProperty(GetSOAP.ENDPOINT_URL, "http://localhost:1080/test_path");
        testRunner.setProperty(GetSOAP.WSDL_URL, "http://localhost:1080/test_path.wsdl");
        testRunner.setProperty(GetSOAP.METHOD_NAME, "testMethod");
        testRunner.setProperty(GetSOAP.USER_NAME, "username");
        testRunner.setProperty(GetSOAP.PASSWORD, "password");
        testRunner.setProperty(GetSOAP.API_NAMESPACE, "http://localhost:1080/");
        testRunner.setProperty(GetSOAP.SKIP_FIRST_ELEMENT, "true");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        List<MockFlowFile> flowFileList = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assert (null != flowFileList);

        final String expectedBody = "<?xml version='1.0'?><dwml version='1.0' xmlns:xsd='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='http://graphical.weather.gov/xml/DWMLgen/schema/DWML.xsd'><latLonList>35.9153,-79.0838</latLonList></dwml>";
        flowFileList.get(0).assertContentEquals(expectedBody.getBytes());

    }

    @Test
    public void testRelationships() {
        GetSOAP getSOAP = new GetSOAP();
        Set<Relationship> relationshipSet = getSOAP.getRelationships();
        assert (null != relationshipSet);
        assert (1 == relationshipSet.size());
        assert (0 == relationshipSet.iterator().next().compareTo(REL_SUCCESS));
    }

    @Test
    public void testGetSoapMethod() {
        final String namespaceUrl = "http://localhost.com/stockquote.wsdl";
        final String namespacePrefix = "nifi";
        final String localName = "testMethod";
        OMElement expectedElement = new OMElementImpl();
        expectedElement.setNamespace(new OMNamespaceImpl(namespaceUrl, namespacePrefix));
        expectedElement.setLocalName(localName);

        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace omNamespace = fac.createOMNamespace(namespaceUrl, namespacePrefix);

        GetSOAP getSOAP = new GetSOAP();
        OMElement element = getSOAP.getSoapMethod(fac, omNamespace, "testMethod");
        assert (null != element);
        assert (namespaceUrl.contentEquals(element.getNamespaceURI()));
        assert (localName.contentEquals(element.getLocalName()));
    }

    @Test
    public void testAddArguments() {
        final String namespaceUrl = "http://localhost.com/stockquote.wsdl";
        final String namespacePrefix = "nifi";
        final String localName = "testMethod";
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace omNamespace = fac.createOMNamespace(namespaceUrl, namespacePrefix);
        OMElement expectedElement = new OMElementImpl();
        expectedElement.setNamespace(new OMNamespaceImpl(namespaceUrl, namespacePrefix));
        expectedElement.setLocalName(localName);

        PropertyDescriptor arg1 = new PropertyDescriptor
                .Builder()
                .name("Argument1")
                .defaultValue("60000")
                .description("The timeout value to use waiting to establish a connection to the web service")
                .dynamic(true)
                .expressionLanguageSupported(false)
                .build();

        testRunner.setProperty(arg1, "111");

        GetSOAP getSOAP = new GetSOAP();
        getSOAP.addArgumentsToMethod(testRunner.getProcessContext(),
                                     fac,
                                     omNamespace,
                                     expectedElement,
                                     new MockFlowFile(1));
        Iterator<OMElement> childItr = expectedElement.getChildElements();
        assert (null != childItr);
        assert (childItr.hasNext());
        assert (arg1.getName().contentEquals(childItr.next().getLocalName()));
        assert (!childItr.hasNext());
    }
}
