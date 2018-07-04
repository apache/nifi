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


package org.apache.nifi.test.samples;



import org.apache.nifi.test.TestNiFiInstance;
import org.apache.nifi.test.util.FileUtils;
import org.apache.nifi.test.SimpleNiFiFlowDefinitionEditor;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.testng.annotations.*;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.List;

import static org.testng.Assert.assertTrue;


/**
 * This test demonstrates how to mock the source data by starting a mock HTTP server (using Jetty)
 * and rewrite the URL in flow definition.
 * <p>
 * NOTE: this test is disabled for now: see @Test(enabled = false)
 */
@Test(enabled=false) // this is a sample for now
public class NiFiFlowTest {

    private static final SimpleNiFiFlowDefinitionEditor CONFIGURE_MOCKS_IN_NIFI_FLOW = SimpleNiFiFlowDefinitionEditor.builder()
            .setSingleProcessorProperty("GetHTTP", "URL", "http://localhost:12345")
            .build();

    // used by mocked GetHTTP; serves test data
    private static Server testJettyServer;

    private final TestNiFiInstance testNiFiInstance;


    public NiFiFlowTest() {

        File nifiZipFile = getBinaryDistributionZipFile(Constants.NIFI_ZIP_DIR);

        this.testNiFiInstance = TestNiFiInstance.builder()
                .setNiFiBinaryDistributionZip(nifiZipFile)
                .setFlowXmlToInstallForTesting(Constants.FLOW_XML_FILE)
                .modifyFlowXmlBeforeInstalling(CONFIGURE_MOCKS_IN_NIFI_FLOW)
                .build();
    }

    private static File getBinaryDistributionZipFile(File binaryDistributionZipDir) {

        File[] files = binaryDistributionZipDir.listFiles((dir, name) ->
                name.startsWith("nifi-") && name.endsWith("-bin.zip"));

        if (files.length == 0) {
            throw new IllegalStateException(
                    "No NiFi distribution ZIP file is found in: " + binaryDistributionZipDir);
        }

        if (files.length != 1) {
            throw new IllegalStateException(
                    "MultipleNiFi distribution ZIP files are found in: " + binaryDistributionZipDir);
        }

        return files[0];
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        NiFiFlowTest.testJettyServer = new Server(12345);


        Handler handler = new TestHandler();
        NiFiFlowTest.testJettyServer.setHandler(handler);
        NiFiFlowTest.testJettyServer.start();
    }


    @BeforeMethod
    public void bootstrapNiFi() throws Exception {

        if (Constants.OUTPUT_DIR.exists()) {
            FileUtils.deleteDirectoryRecursive(Constants.OUTPUT_DIR.toPath());
        }

        testNiFiInstance.install();
        testNiFiInstance.start();
    }

    // test disabled for now: apparenlt NiFi does not like being started
    // multiple times in the same JVM
    @Test
    public void testFlowCreatesFilesInCorrectLocation() throws IOException {

        // We deleted the output directory: our NiFi flow should create it

        assertTrue(Constants.OUTPUT_DIR.exists(), "Output directory not found: " + Constants.OUTPUT_DIR);

        File outputFile = new File(Constants.OUTPUT_DIR, "bbc-world.rss.xml");

        assertTrue(outputFile.exists(), "Output file not found: " + outputFile);

        List<String> strings = Files.readAllLines(outputFile.toPath());

        boolean atLeastOneLineContainsBBC = strings.stream().anyMatch(line -> line.toLowerCase().contains("bbc"));

        assertTrue(atLeastOneLineContainsBBC, "There was no line containing BBC");

        boolean atLeastOneLineContainsIPhone = strings.stream().anyMatch(line -> line.toLowerCase().contains("iphone"));

        assertTrue(atLeastOneLineContainsIPhone, "There was no line containing IPhone");

    }

    @AfterMethod
    public void shutdownNiFi() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        testNiFiInstance.stopAndCleanup();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        NiFiFlowTest.testJettyServer.stop();
    }


    private static class TestHandler extends org.eclipse.jetty.server.handler.AbstractHandler {
        @Override
        public void handle(
                String target,
                Request baseRequest,
                HttpServletRequest httpServletRequest,
                HttpServletResponse response) throws IOException, ServletException {

            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);

            InputStream resource = TestHandler.class.getResourceAsStream("/sample_technology_rss.xml");
            ServletOutputStream outputStream = response.getOutputStream();

            byte[] buffer = new byte[1024];
            int len;
            while ((len = resource.read(buffer)) != -1) {
                outputStream.write(buffer, 0, len);
            }
        }
    }
}
