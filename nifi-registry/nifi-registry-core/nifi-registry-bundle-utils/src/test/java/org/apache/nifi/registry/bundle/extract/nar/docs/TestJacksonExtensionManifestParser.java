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
package org.apache.nifi.registry.bundle.extract.nar.docs;

import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.ExtensionManifest;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.component.manifest.Restriction;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestJacksonExtensionManifestParser {

    private ExtensionManifestParser parser;

    @Before
    public void setup() {
        parser = new JacksonExtensionManifestParser();
    }

    @Test
    public void testDocsWithProcessors() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-hadoop-nar.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.10.0-SNAPSHOT", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();
        assertEquals(10, extensionDetails.size());

        final Extension putHdfsExtension = extensionDetails.stream()
                .filter(e -> e.getName().equals("org.apache.nifi.processors.hadoop.PutHDFS"))
                .findFirst()
                .orElse(null);

        assertNotNull(putHdfsExtension);
        assertEquals(ExtensionType.PROCESSOR, putHdfsExtension.getType());
        assertEquals("Write FlowFile data to Hadoop Distributed File System (HDFS)", putHdfsExtension.getDescription());
        assertEquals(5, putHdfsExtension.getTags().size());
        assertTrue(putHdfsExtension.getTags().contains("hadoop"));
        assertTrue(putHdfsExtension.getTags().contains("HDFS"));
        assertTrue(putHdfsExtension.getTags().contains("put"));
        assertTrue(putHdfsExtension.getTags().contains("copy"));
        assertTrue(putHdfsExtension.getTags().contains("filesystem"));
        assertNull(putHdfsExtension.getProvidedServiceAPIs());

        assertNotNull(putHdfsExtension.getProperties());
        assertEquals(15, putHdfsExtension.getProperties().size());

        assertNull(putHdfsExtension.getRestricted().getGeneralRestrictionExplanation());

        final List<Restriction> restrictions = putHdfsExtension.getRestricted().getRestrictions();
        assertNotNull(restrictions);
        assertEquals(1, restrictions.size());

        final Restriction restriction = restrictions.stream().findFirst().orElse(null);
        assertEquals("write filesystem", restriction.getRequiredPermission());
        assertEquals("Provides operator the ability to delete any file that NiFi has access to in HDFS or\n" +
                "                            the local filesystem.", restriction.getExplanation().trim());
    }

    @Test
    public void testDocsWithControllerService() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-dbcp-service-nar.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.10.0-SNAPSHOT", extensionManifest.getSystemApiVersion());

        final List<Extension> extensions = extensionManifest.getExtensions();
        assertEquals(2, extensions.size());

        final Extension dbcpPoolExtension = extensions.stream()
                .filter(e -> e.getName().equals("org.apache.nifi.dbcp.DBCPConnectionPool"))
                .findFirst()
                .orElse(null);

        assertNotNull(dbcpPoolExtension);
        assertEquals(ExtensionType.CONTROLLER_SERVICE, dbcpPoolExtension.getType());
        assertEquals("Provides Database Connection Pooling Service. Connections can be asked from pool and returned\n" +
                "                after usage.", dbcpPoolExtension.getDescription().trim());
        assertEquals(6, dbcpPoolExtension.getTags().size());
        assertEquals(1, dbcpPoolExtension.getProvidedServiceAPIs().size());

        final ProvidedServiceAPI providedServiceApi = dbcpPoolExtension.getProvidedServiceAPIs().iterator().next();
        assertNotNull(providedServiceApi);
        assertEquals("org.apache.nifi.dbcp.DBCPService", providedServiceApi.getClassName());
        assertEquals("org.apache.nifi", providedServiceApi.getGroupId());
        assertEquals("nifi-standard-services-api-nar", providedServiceApi.getArtifactId());
        assertEquals("1.10.0-SNAPSHOT", providedServiceApi.getVersion());
    }

    @Test
    public void testDocsWithReportingTask() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-ambari-nar.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.10.0-SNAPSHOT", extensionManifest.getSystemApiVersion());

        final List<Extension> extensions = extensionManifest.getExtensions();
        assertEquals(1, extensions.size());

        final Extension reportingTask = extensions.stream()
                .filter(e -> e.getName().equals("org.apache.nifi.reporting.ambari.AmbariReportingTask"))
                .findFirst()
                .orElse(null);

        assertNotNull(reportingTask);
        assertEquals(ExtensionType.REPORTING_TASK, reportingTask.getType());
        assertNotNull(reportingTask.getDescription());
        assertEquals(3, reportingTask.getTags().size());
        assertTrue(reportingTask.getTags().contains("reporting"));
        assertTrue(reportingTask.getTags().contains("metrics"));
        assertTrue(reportingTask.getTags().contains("ambari"));
        assertNull(reportingTask.getProvidedServiceAPIs());
    }

    @Test
    public void testDocsForTestComponents() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-test-components.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.8.0", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();
        assertEquals(4, extensionDetails.size());

    }

    @Test
    public void testDocsForMissingSystemApi() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-missing-sys-api.xml");
        assertNotNull(extensionManifest);
    }

    @Test
    public void testDocsForUnknownProperties() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/descriptors/extension-manifest-unknown-property.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.8.0", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();
        assertEquals(1, extensionDetails.size());

    }

    private ExtensionManifest parse(final String file) throws IOException {
        try (final InputStream inputStream = new FileInputStream(file)) {
            return parser.parse(inputStream);
        }
    }
}
