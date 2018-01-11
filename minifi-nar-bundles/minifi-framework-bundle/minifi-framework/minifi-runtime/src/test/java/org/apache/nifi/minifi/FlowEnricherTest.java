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

package org.apache.nifi.minifi;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails.Builder;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;

public class FlowEnricherTest {

    private static final String TEST_FLOW_XML_PATH = "./src/test/resources/flow.xml.gz";
    private static final File TEST_FLOW_XML_FILE = new File(TEST_FLOW_XML_PATH);

    private static final String KAFKA_PROCESSOR_CLASS = "org.apache.nifi.processors.kafka.pubsub.PublishKafka_0_10";
    private static final String TAILFILE_PROCESSOR_CLASS = "org.apache.nifi.processors.standard.TailFile";
    private static final String SPLITCONTENT_PROCESSOR_CLASS = "org.apache.nifi.processors.standard.SplitContent";
    private static final String SSL_CONTROLLER_SERVICE_CLASS = "org.apache.nifi.ssl.StandardSSLContextService";

    private static final List<String> EXTENSION_CLASSES = Arrays.asList(
            KAFKA_PROCESSOR_CLASS,
            TAILFILE_PROCESSOR_CLASS,
            SPLITCONTENT_PROCESSOR_CLASS,
            SSL_CONTROLLER_SERVICE_CLASS);

    private static ClassLoader mockClassLoader = Mockito.mock(ClassLoader.class);

    @Test
    public void enrichFlowWithBundleInformationTest() throws Exception {

        /* Setup Mocks */
        // Create a copy of the document to use with our mock
        final FlowParser parser = new FlowParser();
        final Document flowDocument = parser.parse(TEST_FLOW_XML_FILE);

        final FlowParser mockFlowParser = Mockito.mock(FlowParser.class);
        Mockito.when(mockFlowParser.parse(any())).thenReturn(flowDocument);

        final MiNiFi minifi = Mockito.mock(MiNiFi.class);

        final NiFiProperties mockProperties = Mockito.mock(NiFiProperties.class);
        Mockito.when(mockProperties.getFlowConfigurationFile()).thenReturn(new File(TEST_FLOW_XML_PATH));

        final String defaultGroup = "org.apache.nifi";
        final String aVersion = "1.0.0";
        final String anotherVersion = "2.0.0";

        final String minifiGroup = "org.apache.nifi.minifi";
        final String minifiStandardNarId = "minifi-standard-nar";
        final String minifiVersion = "0.0.2";

        // Standard Services API Bundles
        final String standardSvcsId = "nifi-standard-services-api-nar";
        final List<Bundle> standardSvcsBundles = new ArrayList<>();
        standardSvcsBundles.add(generateBundle(defaultGroup, standardSvcsId, aVersion));
        standardSvcsBundles.add(generateBundle(defaultGroup, standardSvcsId, anotherVersion));

        // SSL Context Service Bundles - depends on nifi-standard-services-api
        final String sslContextSvcId = "nifi-ssl-context-service-nar";
        final List<Bundle> sslBundles = new ArrayList<>();
        sslBundles.add(generateBundle(defaultGroup, sslContextSvcId, aVersion, standardSvcsBundles.get(0).getBundleDetails().getCoordinate()));
        sslBundles.add(generateBundle(defaultGroup, sslContextSvcId, anotherVersion, standardSvcsBundles.get(1).getBundleDetails().getCoordinate()));

        // MiNiFi Standard NAR Bundle
        List<Bundle> minifiStdBundles = new ArrayList<>();
        minifiStdBundles.add(generateBundle(minifiGroup, minifiStandardNarId, minifiVersion, standardSvcsBundles.get(0).getBundleDetails().getCoordinate()));

        // Kafka depends on SSL
        List<Bundle> kafkaBundles = new ArrayList<>();
        final String kafkaId = "nifi-kafka-0-10-nar";
        kafkaBundles.add(generateBundle(defaultGroup, kafkaId, anotherVersion, standardSvcsBundles.get(1).getBundleDetails().getCoordinate()));


       /* If we are evaluating potential problem children components, provide a tailored bundle.
        * Otherwise, these can be sourced from the standard NAR.
        */
        Mockito.when(minifi.getBundles(anyString())).thenAnswer((Answer<List<Bundle>>) invocationOnMock -> {
            final String requestedClass = (String) invocationOnMock.getArguments()[0];
            switch (requestedClass) {
                case KAFKA_PROCESSOR_CLASS:
                    return kafkaBundles;
                case SSL_CONTROLLER_SERVICE_CLASS:
                    return sslBundles;
                default:
                    return minifiStdBundles;
            }
        });

        /* Perform Test */
        final FlowEnricher flowEnricher = new FlowEnricher(minifi, mockFlowParser, mockProperties);
        flowEnricher.enrichFlowWithBundleInformation();

        final ArgumentCaptor<String> bundleLookupCaptor = ArgumentCaptor.forClass(String.class);

        // Inspect the document to ensure all components were enriched
        Mockito.verify(minifi, atLeastOnce()).getBundles(bundleLookupCaptor.capture());
        final List<String> allValues = bundleLookupCaptor.getAllValues();

        // Verify each class performed a bundle look up
        EXTENSION_CLASSES.stream().forEach(procClass -> Assert.assertTrue(allValues.contains(procClass)));

        // Verify bundles are correctly applied in our flow document
        // We have three processors, one controller service, and no reporting tasks
        final NodeList processorNodes = flowDocument.getElementsByTagName(FlowEnricher.PROCESSOR_TAG_NAME);
        Assert.assertEquals("Incorrect number of processors", 3, processorNodes.getLength());

        final NodeList controllerServiceNodes = flowDocument.getElementsByTagName(FlowEnricher.CONTROLLER_SERVICE_TAG_NAME);
        Assert.assertEquals("Incorrect number of controller services", 1, controllerServiceNodes.getLength());

        final NodeList reportingTaskNodes = flowDocument.getElementsByTagName(FlowEnricher.REPORTING_TASK_TAG_NAME);
        Assert.assertEquals("Incorrect number of reporting tasks", 0, reportingTaskNodes.getLength());

        for (int i = 0; i < processorNodes.getLength(); i++) {
            final Element componentElement = (Element) processorNodes.item(i);
            Assert.assertEquals(1, componentElement.getElementsByTagName(FlowEnricher.EnrichingElementAdapter.BUNDLE_ELEMENT_NAME).getLength());
            final FlowEnricher.EnrichingElementAdapter elementAdapter = new FlowEnricher.EnrichingElementAdapter(componentElement);

            // Only our Kafka processor has a bundle outside of the standard bundle
            if (elementAdapter.getComponentClass().equalsIgnoreCase(KAFKA_PROCESSOR_CLASS)) {
                verifyBundleProperties(elementAdapter, defaultGroup, kafkaId, anotherVersion);
            } else {
                verifyBundleProperties(elementAdapter, minifiGroup, minifiStandardNarId, minifiVersion);
            }
        }

        for (int i = 0; i < controllerServiceNodes.getLength(); i++) {
            final Element componentElement = (Element) controllerServiceNodes.item(i);
            Assert.assertEquals(1, componentElement.getElementsByTagName(FlowEnricher.EnrichingElementAdapter.BUNDLE_ELEMENT_NAME).getLength());
            final FlowEnricher.EnrichingElementAdapter elementAdapter = new FlowEnricher.EnrichingElementAdapter(componentElement);

            // Only our Kafka processor has a bundle outside of the standard bundle
            if (elementAdapter.getComponentClass().equalsIgnoreCase(SSL_CONTROLLER_SERVICE_CLASS)) {
                verifyBundleProperties(elementAdapter, defaultGroup, sslContextSvcId, anotherVersion);
            } else {
                Assert.fail("A controller service that was not an SSL Controller service was found.");
            }
        }

        // Verify the updated flow was persisted
        Mockito.verify(mockFlowParser, atLeastOnce()).writeFlow(any(), any());
    }

    /* utility methods to generate and verify test objects */

    private static Bundle generateBundle(String group, String id, String version) {
        return generateBundle(group, id, version, null, null, null);
    }


    private static Bundle generateBundle(String group, String id, String version, String dependencyGroup, String dependencyId, String dependencyVersion) {
        final BundleCoordinate dependencyCoordinate =
                (dependencyGroup != null && dependencyId != null && dependencyVersion != null)
                        ? new BundleCoordinate(dependencyGroup, dependencyId, dependencyVersion) : null;

        return generateBundle(group, id, version, dependencyCoordinate);
    }

    private static Bundle generateBundle(String group, String id, String version, BundleCoordinate dependencyCoordinate) {

        final File workingDirectory = new File("src/test/resources");

        final BundleCoordinate bundleCoordinate = new BundleCoordinate(group, id, version);

        final String buildTag = "HEAD";
        final String buildRevision = "1";
        final String buildBranch = "DEV";
        final String buildTimestamp = "2017-01-01 00:00:00";
        final String buildJdk = "JDK8";
        final String builtBy = "somebody";

        Builder bundleBuilder = new Builder()
                .workingDir(workingDirectory)
                .coordinate(bundleCoordinate)
                .buildTag(buildTag)
                .buildRevision(buildRevision)
                .buildBranch(buildBranch)
                .buildTimestamp(buildTimestamp)
                .buildJdk(buildJdk)
                .builtBy(builtBy);

        if (dependencyCoordinate != null) {
            bundleBuilder.dependencyCoordinate(dependencyCoordinate);
        }
        return new Bundle(bundleBuilder.build(), mockClassLoader);
    }

    private static void verifyBundleProperties(FlowEnricher.EnrichingElementAdapter capturedComponent, String expectedGroup, String expectedArtifact, String expectedVersion) {
        Assert.assertEquals("Wrong group for component", expectedGroup, capturedComponent.getBundleGroup());
        Assert.assertEquals("Wrong id for component", expectedArtifact, capturedComponent.getBundleId());
        Assert.assertEquals("Wrong version for component", expectedVersion, capturedComponent.getBundleVersion());
    }
}
