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
package org.apache.nifi.fingerprint;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.controller.serialization.ScheduledStateLookup;
import org.apache.nifi.controller.serialization.StandardFlowSerializer;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.SensitiveValueEncoder;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.security.xml.XmlUtils;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

import static org.apache.nifi.controller.serialization.ScheduledStateLookup.IDENTITY_LOOKUP;
import static org.apache.nifi.fingerprint.FingerprintFactory.FLOW_CONFIG_XSD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class FingerprintFactoryTest {

    private PropertyEncryptor encryptor;
    private ExtensionManager extensionManager;
    private FingerprintFactory fingerprintFactory;
    private SensitiveValueEncoder sensitiveValueEncoder;

    @Before
    public void setup() {
        encryptor = createEncryptor();
        sensitiveValueEncoder = createSensitiveValueEncoder();
        extensionManager = new StandardExtensionDiscoveringManager();
        fingerprintFactory = new FingerprintFactory(encryptor, extensionManager, sensitiveValueEncoder);
    }

    @Test
    public void testSameFingerprint() throws IOException {
        final String fp1 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1b.xml"), null);
        assertEquals(fp1, fp2);
    }

    @Test
    public void testDifferentFingerprint() throws IOException {
        final String fp1 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow2.xml"), null);
        assertNotEquals(fp1, fp2);
    }

    @Test
    public void testResourceValueInFingerprint() throws IOException {
        final String fingerprint = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        assertEquals(3, StringUtils.countMatches(fingerprint, "success"));
        assertTrue(fingerprint.contains("In Connection"));
    }

    @Test
    public void testSameFlowWithDifferentBundleShouldHaveDifferentFingerprints() throws IOException {
        final String fp1 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-bundle-1.xml"), null);
        assertTrue(fp1.contains("org.apache.nifinifi-standard-nar1.0"));

        final String fp2 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-bundle-2.xml"), null);
        assertTrue(fp2.contains("org.apache.nifinifi-standard-nar2.0"));

        assertNotEquals(fp1, fp2);
    }

    @Test
    public void testSameFlowAndOneHasNoBundleShouldHaveDifferentFingerprints() throws IOException {
        final String fp1 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-bundle-1.xml"), null);
        assertTrue(fp1.contains("org.apache.nifinifi-standard-nar1.0"));

        final String fp2 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-no-bundle.xml"), null);
        assertTrue(fp2.contains("MISSING_BUNDLE"));

        assertNotEquals(fp1, fp2);
    }

    @Test
    public void testSameFlowAndOneHasMissingBundleShouldHaveDifferentFingerprints() throws IOException {
        final String fp1 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-bundle-1.xml"), null);
        assertTrue(fp1.contains("org.apache.nifinifi-standard-nar1.0"));

        final String fp2 = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow3-with-missing-bundle.xml"), null);
        assertTrue(fp2.contains("missingmissingmissing"));

        assertNotEquals(fp1, fp2);
    }

    @Test
    public void testConnectionWithMultipleRelationshipsSortedInFingerprint() throws IOException {
        final String fingerprint = fingerprintFactory.createFingerprint(getResourceBytes("/nifi/fingerprint/flow-connection-with-multiple-rels.xml"), null);
        assertNotNull(fingerprint);
        assertTrue(fingerprint.contains("AAABBBCCCDDD"));
    }

    @Test
    public void testSchemaValidation() throws IOException {
        FingerprintFactory fp = new FingerprintFactory(null, getValidatingDocumentBuilder(), extensionManager, null);
        fp.createFingerprint(getResourceBytes("/nifi/fingerprint/validating-flow.xml"), null);
    }

    private byte[] getResourceBytes(final String resource) throws IOException {
        return IOUtils.toByteArray(FingerprintFactoryTest.class.getResourceAsStream(resource));
    }

    private DocumentBuilder getValidatingDocumentBuilder() {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema;
        try {
            schema = schemaFactory.newSchema(FingerprintFactory.class.getResource(FLOW_CONFIG_XSD));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse schema for file flow configuration.", e);
        }
        try {
            DocumentBuilder docBuilder = XmlUtils.createSafeDocumentBuilder(schema, true);
            docBuilder.setErrorHandler(new ErrorHandler() {
                @Override
                public void warning(SAXParseException e) throws SAXException {
                    throw e;
                }

                @Override
                public void error(SAXParseException e) throws SAXException {
                    throw e;
                }

                @Override
                public void fatalError(SAXParseException e) throws SAXException {
                    throw e;
                }
            });
            return docBuilder;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create document builder for flow configuration.", e);
        }
    }

    private <T> Element serializeElement(final PropertyEncryptor encryptor, final Class<T> componentClass, final T component,
                                         final String serializerMethodName, ScheduledStateLookup scheduledStateLookup) throws Exception {
        final DocumentBuilder docBuilder = XmlUtils.createSafeDocumentBuilder(false);
        final Document doc = docBuilder.newDocument();

        final FlowSerializer flowSerializer = new StandardFlowSerializer(encryptor);
        final Method serializeMethod = StandardFlowSerializer.class.getDeclaredMethod(serializerMethodName,
                Element.class, componentClass, ScheduledStateLookup.class);
        serializeMethod.setAccessible(true);
        final Element rootElement = doc.createElement("root");
        serializeMethod.invoke(flowSerializer, rootElement, component, scheduledStateLookup);
        return rootElement;
    }

    private <T> String fingerprint(final String methodName, final Class<T> inputClass, final T input) throws Exception {
        final Method fingerprintFromComponent = FingerprintFactory.class.getDeclaredMethod(methodName,
                StringBuilder.class, inputClass);
        fingerprintFromComponent.setAccessible(true);

        final StringBuilder fingerprint = new StringBuilder();
        fingerprintFromComponent.invoke(fingerprintFactory, fingerprint, input);
        return fingerprint.toString();
    }

    @Test
    public void testRemoteProcessGroupFingerprintRaw() throws Exception {

        // Fill out every configuration.
        final RemoteProcessGroup component = mock(RemoteProcessGroup.class);
        when(component.getName()).thenReturn("name");
        when(component.getIdentifier()).thenReturn("id");
        when(component.getPosition()).thenReturn(new Position(10.5, 20.3));
        when(component.getTargetUri()).thenReturn("http://node1:8080/nifi");
        when(component.getTargetUris()).thenReturn("http://node1:8080/nifi, http://node2:8080/nifi");
        when(component.getNetworkInterface()).thenReturn("eth0");
        when(component.getComments()).thenReturn("comment");
        when(component.getCommunicationsTimeout()).thenReturn("10 sec");
        when(component.getYieldDuration()).thenReturn("30 sec");
        when(component.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.RAW);
        when(component.getProxyHost()).thenReturn(null);
        when(component.getProxyPort()).thenReturn(null);
        when(component.getProxyUser()).thenReturn(null);
        when(component.getProxyPassword()).thenReturn(null);
        when(component.getVersionedComponentId()).thenReturn(Optional.empty());

        // Assert fingerprints with expected one.
        final String expected = "id" +
                "NO_VALUE" +
                "http://node1:8080/nifi, http://node2:8080/nifi" +
                "eth0" +
                "10 sec" +
                "30 sec" +
                "RAW" +
                "NO_VALUE" +
                "NO_VALUE" +
                "NO_VALUE" +
                "NO_VALUE";

        final Element rootElement = serializeElement(encryptor, RemoteProcessGroup.class, component, "addRemoteProcessGroup", IDENTITY_LOOKUP);
        final Element componentElement = (Element) rootElement.getElementsByTagName("remoteProcessGroup").item(0);
        assertEquals(expected, fingerprint("addRemoteProcessGroupFingerprint", Element.class, componentElement));

    }

    @Test
    public void testRemoteProcessGroupFingerprintWithProxy() throws Exception {
        final String proxyPassword = "proxy-pass";

        // Fill out every configuration.
        final RemoteProcessGroup component = mock(RemoteProcessGroup.class);
        when(component.getName()).thenReturn("name");
        when(component.getIdentifier()).thenReturn("id");
        when(component.getPosition()).thenReturn(new Position(10.5, 20.3));
        when(component.getTargetUri()).thenReturn("http://node1:8080/nifi");
        when(component.getTargetUris()).thenReturn("http://node1:8080/nifi, http://node2:8080/nifi");
        when(component.getComments()).thenReturn("comment");
        when(component.getCommunicationsTimeout()).thenReturn("10 sec");
        when(component.getYieldDuration()).thenReturn("30 sec");
        when(component.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(component.getProxyHost()).thenReturn("proxy-host");
        when(component.getProxyPort()).thenReturn(3128);
        when(component.getProxyUser()).thenReturn("proxy-user");
        when(component.getProxyPassword()).thenReturn(proxyPassword);
        when(component.getVersionedComponentId()).thenReturn(Optional.empty());

        final String hashedProxyPassword = sensitiveValueEncoder.getEncoded(proxyPassword);

        // Assert fingerprints with expected one.
        final String expected = "id" +
                "NO_VALUE" +
                "http://node1:8080/nifi, http://node2:8080/nifi" +
                "NO_VALUE" +
                "10 sec" +
                "30 sec" +
                "HTTP" +
                "proxy-host" +
                "3128" +
                "proxy-user" +
                hashedProxyPassword;

        final Element rootElement = serializeElement(encryptor, RemoteProcessGroup.class, component, "addRemoteProcessGroup", IDENTITY_LOOKUP);
        final Element componentElement = (Element) rootElement.getElementsByTagName("remoteProcessGroup").item(0);
        assertEquals(expected, fingerprint("addRemoteProcessGroupFingerprint", Element.class, componentElement));
    }

    @Test
    public void testRemotePortFingerprint() throws Exception {

        // Fill out every configuration.
        final RemoteProcessGroup groupComponent = mock(RemoteProcessGroup.class);
        when(groupComponent.getName()).thenReturn("name");
        when(groupComponent.getIdentifier()).thenReturn("id");
        when(groupComponent.getPosition()).thenReturn(new Position(10.5, 20.3));
        when(groupComponent.getTargetUri()).thenReturn("http://node1:8080/nifi");
        when(groupComponent.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.RAW);
        when(groupComponent.getVersionedComponentId()).thenReturn(Optional.empty());

        final RemoteGroupPort portComponent = mock(RemoteGroupPort.class);
        when(groupComponent.getInputPorts()).thenReturn(Collections.singleton(portComponent));
        when(portComponent.getName()).thenReturn("portName");
        when(portComponent.getIdentifier()).thenReturn("portId");
        when(portComponent.getPosition()).thenReturn(new Position(10.5, 20.3));
        when(portComponent.getComments()).thenReturn("portComment");
        when(portComponent.getScheduledState()).thenReturn(ScheduledState.RUNNING);
        when(portComponent.getMaxConcurrentTasks()).thenReturn(3);
        when(portComponent.isUseCompression()).thenReturn(true);
        when(portComponent.getBatchCount()).thenReturn(1234);
        when(portComponent.getBatchSize()).thenReturn("64KB");
        when(portComponent.getBatchDuration()).thenReturn("10sec");
        // Serializer doesn't serialize if a port doesn't have any connection.
        when(portComponent.hasIncomingConnection()).thenReturn(true);
        when(portComponent.getVersionedComponentId()).thenReturn(Optional.empty());

        // Assert fingerprints with expected one.
        final String expected = "portId" +
                "NO_VALUE" +
                "NO_VALUE" +
                "3" +
                "true" +
                "1234" +
                "64KB" +
                "10sec";

        final Element rootElement = serializeElement(encryptor, RemoteProcessGroup.class, groupComponent, "addRemoteProcessGroup", IDENTITY_LOOKUP);
        final Element componentElement = (Element) rootElement.getElementsByTagName("inputPort").item(0);
        assertEquals(expected, fingerprint("addRemoteGroupPortFingerprint", Element.class, componentElement));
    }

    @Test
    public void testControllerServicesIncludedInGroupFingerprint() throws ParserConfigurationException, IOException, SAXException {
        final DocumentBuilder docBuilder = XmlUtils.createSafeDocumentBuilder(false);
        final Document document = docBuilder.parse(new File("src/test/resources/nifi/fingerprint/group-with-controller-services.xml"));
        final Element processGroup = document.getDocumentElement();

        final StringBuilder sb = new StringBuilder();
        fingerprintFactory.addProcessGroupFingerprint(sb, processGroup, new FlowEncodingVersion(1, 0));

        final String fingerprint = sb.toString();
        final String[] criticalFingerprintValues = new String[] {
            "1234",
            "s1", "service1", "prop1", "value1", "org.apache.nifi.services.FingerprintControllerService",
            "s2", "service2", "another property", "another value", "org.apache.nifi.services.AnotherService",
        };

        for (final String criticalValue : criticalFingerprintValues) {
            assertTrue("Fingerprint did not contain '" + criticalValue + "'", fingerprint.contains(criticalValue));
        }

        // Ensure that 's1' comes before 's2' in the fingerprint
        assertTrue(fingerprint.indexOf("FingerprintControllerService") < fingerprint.indexOf("AnotherService"));
    }

    private PropertyEncryptor createEncryptor() {
        return new PropertyEncryptor() {
            @Override
            public String encrypt(String property) {
                return Hex.encodeHexString(property.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public String decrypt(String encryptedProperty) {
                try {
                    return new String(Hex.decodeHex(encryptedProperty));
                } catch (DecoderException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    private SensitiveValueEncoder createSensitiveValueEncoder() {
        return new SensitiveValueEncoder() {
            @Override
            public String getEncoded(String sensitivePropertyValue) {
                return String.format("[MASKED] %s", sensitivePropertyValue);
            }
        };
    }
}
