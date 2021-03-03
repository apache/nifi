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

import com.scientiamobile.wurfl.wmclient.Model;
import com.scientiamobile.wurfl.wmclient.WmClient;
import com.scientiamobile.wurfl.wmclient.WmException;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WURFLDeviceEnrichProcessor.class})
public class TestWURFLDeviceEnrichProcessor {

    WmClient wmClient;
    WURFLDeviceEnrichProcessor wurflDeviceEnrich;
    private TestRunner testRunner;

    @Before
    public void setUp() {
        wmClient = mock(WmClient.class);
        wurflDeviceEnrich = new TestableWURFLDeviceEnrich();
        wurflDeviceEnrich.wmClientRef = new AtomicReference<>();
        testRunner = TestRunners.newTestRunner(wurflDeviceEnrich);
    }

    @Test
    public void testMissingPropertyValues() {

        setValidTestPropertiesToRunner();
        testRunner.removeProperty(WURFLDeviceEnrichProcessor.WM_HOST);
        testRunner.assertNotValid();
        setValidTestPropertiesToRunner();
        testRunner.removeProperty(WURFLDeviceEnrichProcessor.WM_PORT);
        testRunner.assertNotValid();
        testRunner.removeProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_HTTP_HEADERS_PREFIX);
        testRunner.assertNotValid();

    }

    private void setValidTestPropertiesToRunner() {
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_SCHEME, "http");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_HOST, "localhost");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_PORT, "9080");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_USER_AGENT, "attribute name");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_HTTP_HEADERS_PREFIX, "http.headers.User-Agent");
    }

    @Test
    public void wmExceptionFlowsToFailure() throws WmException {

        setValidTestPropertiesToRunner();

        String ua = "Mozilla/5.0 (Linux; Android 10; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.62 Mobile Safari/537.36";
        Map<String,String> headers = new HashMap<>();
        headers.put("User-Agent", ua);
        WmException e = new WmException("Detection failed");
        when(wmClient.lookupHeaders(headers)).thenThrow(e);
        when(wmClient.getImportantHeaders()).thenReturn(mockImportantHeaders());
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("http.headers.User-Agent", ua);

        testRunner.enqueue(new byte[0], attributes);
        testRunner.run();

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.FAILURE);
        assertEquals(1, failure.size());
        String failAttr = failure.get(0).getAttribute(WURFLDeviceEnrichProcessor.FAILURE_ATTR_NAME);
        assertNotNull(failAttr);
        assertEquals(failAttr, "Detection failed");
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.SUCCESS);
        assertEquals(0, success.size());
    }

    @Test
    public void invalidHeaderPrefixReturnsGenericDevice() throws WmException {

        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_SCHEME, "http");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_HOST, "localhost");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_PORT, "9080");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_USER_AGENT, "attribute name prefix");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_HTTP_HEADERS_PREFIX, "invalid.prefix.");

        when(wmClient.getImportantHeaders()).thenReturn(mockImportantHeaders());
        Model.JSONDeviceData d = createMockDevice("generic", "", "", "false", "false");

        Map<String,String> headers = new HashMap<>();

        when(wmClient.lookupHeaders(headers)).thenReturn(d);
        final Map<String, String> attributes = new HashMap<>();
        // While this is a valid user agent. it will not selected for detection because processor has been configured to read prefixed headers
        attributes.put("User-Agent", "AdsBot-Sample (+http://www.sample.c_om/adsbot)");
        attributes.put("Accept", "text/html");
        attributes.put("X-custom", "custom_value");

        testRunner.enqueue(new byte[0], attributes);
        testRunner.run();

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.FAILURE);
        assertEquals(0, failure.size());
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.SUCCESS);
        assertEquals(1, success.size());

        assertEquals("generic", success.get(0).getAttribute("wurfl.wurfl_id"));
        assertEquals("false", success.get(0).getAttribute("wurfl.is_smartphone"));
        assertEquals("false", success.get(0).getAttribute("wurfl.is_robot"));
    }

    @Test
    public void detectionUsingAttributeNameSuccess() throws WmException {

        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_SCHEME, "http");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_HOST, "localhost");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_PORT, "9080");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_USER_AGENT, "attribute name");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_HTTP_HEADERS_PREFIX, "User-Agent");

        String ua = "Mozilla/5.0 (Linux; Android 10; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.62 Mobile Safari/537.36";
        Map<String,String> headers = new HashMap<>();
        headers.put("User-Agent", ua);
        Model.JSONDeviceData d = createMockDevice("google_pixel_4_xl_ver1", "Google", "Google Pixel 4 XL",
                "true", "false");
        when(wmClient.getImportantHeaders()).thenReturn(mockImportantHeaders());
        when(wmClient.lookupHeaders(headers)).thenReturn(d);
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("User-Agent", ua);

        testRunner.enqueue(new byte[0], attributes);
        testRunner.run();

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.FAILURE);
        assertEquals(0, failure.size());
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.SUCCESS);
        assertNotNull(success);
        assertEquals(1, success.size());
        assertEquals("Google", success.get(0).getAttribute("wurfl.brand_name"));
        assertEquals("Google Pixel 4 XL", success.get(0).getAttribute("wurfl.complete_device_name"));
        assertEquals("google_pixel_4_xl_ver1", success.get(0).getAttribute("wurfl.wurfl_id"));
        assertEquals("true", success.get(0).getAttribute("wurfl.is_smartphone"));
        assertEquals("false", success.get(0).getAttribute("wurfl.is_robot"));
    }

    @Test
    public void detectionUsingAttributeNamePrefixSuccess() throws WmException {

        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_SCHEME, "http");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_HOST, "localhost");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.WM_PORT, "9080");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_USER_AGENT, "attribute name prefix");
        testRunner.setProperty(WURFLDeviceEnrichProcessor.INPUT_ATTR_HTTP_HEADERS_PREFIX, "http.headers.");

        String ua = "Mozilla/5.0 (Linux; Android 10; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.62 Mobile Safari/537.36";
        Map<String,String> headers = new HashMap<>();
        headers.put("User-Agent", ua);
        Model.JSONDeviceData d = createMockDevice("google_pixel_4_xl_ver1", "Google", "Google Pixel 4 XL",
                "true", "false");
        when(wmClient.getImportantHeaders()).thenReturn(mockImportantHeaders());
        when(wmClient.lookupHeaders(headers)).thenReturn(d);
        final Map<String, String> attributes = new HashMap<>();

        attributes.put("http.headers.User-Agent", ua);
        attributes.put("http.headers.Connection", "keep-alive");
        attributes.put("http.headers.Accept-Encoding", "gzip");

        testRunner.enqueue(new byte[0], attributes);
        testRunner.run();

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.FAILURE);
        assertEquals(0, failure.size());
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(WURFLDeviceEnrichProcessor.SUCCESS);
        assertNotNull(success);
        assertEquals(1, success.size());
        assertEquals("Google", success.get(0).getAttribute("wurfl.brand_name"));
        assertEquals("Google Pixel 4 XL", success.get(0).getAttribute("wurfl.complete_device_name"));
        assertEquals("google_pixel_4_xl_ver1", success.get(0).getAttribute("wurfl.wurfl_id"));
        assertEquals("true", success.get(0).getAttribute("wurfl.is_smartphone"));
        assertEquals("false", success.get(0).getAttribute("wurfl.is_robot"));
    }

    private String[] mockImportantHeaders() {
        String[] importantHeaders = new String[4];
        importantHeaders[0]  = "User-Agent";
        importantHeaders[1] = "Device-Stock-UA";
        importantHeaders[2] = "X-OperaMini-Phone-UA";
        importantHeaders[3] = "X-UCBrowser-Device-UA";
        return importantHeaders;
    }

    private Model.JSONDeviceData createMockDevice(String wurflId, String brandName, String completeDeviceName, String isSmartphone, String isRobot) {
        Map<String, String> capabilities = new HashMap<>();
        capabilities.put("brand_name", brandName);
        capabilities.put("complete_device_name", completeDeviceName);
        capabilities.put("wurfl_id", wurflId);
        capabilities.put("is_smartphone", isSmartphone);
        capabilities.put("is_robot", isRobot);
        Model.JSONDeviceData d = new Model().new JSONDeviceData(capabilities, "", 0);
        return d;
    }

    class TestableWURFLDeviceEnrich extends WURFLDeviceEnrichProcessor {
        @OnScheduled
        public void onScheduled(ProcessContext context) {
            wmClientRef.set(wmClient);
        }
    }

}
