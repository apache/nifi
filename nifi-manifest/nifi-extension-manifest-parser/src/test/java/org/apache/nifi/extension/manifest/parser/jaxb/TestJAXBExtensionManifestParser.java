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
package org.apache.nifi.extension.manifest.parser.jaxb;

import org.apache.nifi.extension.manifest.Cardinality;
import org.apache.nifi.extension.manifest.DefaultSchedule;
import org.apache.nifi.extension.manifest.DefaultSettings;
import org.apache.nifi.extension.manifest.Dependency;
import org.apache.nifi.extension.manifest.DependentValues;
import org.apache.nifi.extension.manifest.Extension;
import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.Property;
import org.apache.nifi.extension.manifest.ProvidedServiceAPI;
import org.apache.nifi.extension.manifest.ResourceDefinition;
import org.apache.nifi.extension.manifest.ResourceType;
import org.apache.nifi.extension.manifest.Restriction;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJAXBExtensionManifestParser {

    private ExtensionManifestParser parser;

    @BeforeEach
    public void setup() {
        parser = new JAXBExtensionManifestParser();
    }

    @Test
    public void testDocsWithProcessors() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-hadoop-nar.xml");
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
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-dbcp-service-nar.xml");
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
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-ambari-nar.xml");
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
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-test-components.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.8.0", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();
        assertEquals(4, extensionDetails.size());

        final Extension processor1 = extensionDetails.stream()
                .filter(extension -> extension.getName().equals("org.apache.nifi.processors.TestProcessor1"))
                .findFirst()
                .orElse(null);
        assertNotNull(processor1);
        assertTrue(processor1.getTriggerSerially());
        assertTrue(processor1.getTriggerWhenEmpty());
        assertTrue(processor1.getTriggerWhenAnyDestinationAvailable());
        assertTrue(processor1.getPrimaryNodeOnly());
        assertTrue(processor1.getEventDriven());
        assertTrue(processor1.getSupportsBatching());
        assertTrue(processor1.getSideEffectFree());

        final DefaultSettings defaultSettingsProc1 = processor1.getDefaultSettings();
        assertNotNull(defaultSettingsProc1);
        assertEquals("10 secs", defaultSettingsProc1.getYieldDuration());
        assertEquals("20 secs", defaultSettingsProc1.getPenaltyDuration());
        assertEquals("DEBUG", defaultSettingsProc1.getBulletinLevel());

        final DefaultSchedule defaultScheduleProc1 = processor1.getDefaultSchedule();
        assertNotNull(defaultScheduleProc1);
        assertEquals("CRON_DRIVEN", defaultScheduleProc1.getStrategy());
        assertEquals("* 1 * * *", defaultScheduleProc1.getPeriod());
        assertEquals("5", defaultScheduleProc1.getConcurrentTasks());

        final Extension processor2 = extensionDetails.stream()
                .filter(extension -> extension.getName().equals("org.apache.nifi.processors.TestProcessor2"))
                .findFirst()
                .orElse(null);
        assertNotNull(processor2);
        assertFalse(processor2.getTriggerSerially());
        assertFalse(processor2.getTriggerWhenEmpty());
        assertFalse(processor2.getTriggerWhenAnyDestinationAvailable());
        assertFalse(processor2.getPrimaryNodeOnly());
        assertFalse(processor2.getEventDriven());
        assertFalse(processor2.getSupportsBatching());
        assertFalse(processor2.getSideEffectFree());

        assertNull(processor2.getDefaultSchedule());
        assertNull(processor2.getDefaultSettings());
    }

    @Test
    public void testDocsForMissingSystemApi() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-missing-sys-api.xml");
        assertNotNull(extensionManifest);
    }

    @Test
    public void testDocsForUnknownProperties() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-unknown-property.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.8.0", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();
        assertEquals(1, extensionDetails.size());

    }

    @Test
    public void testDocsWithDependentProperties() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-kafka-2-6-nar.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.16.0-SNAPSHOT", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();

        final Extension consumeKafkaExtension = extensionDetails.stream()
                .filter(e -> e.getName().equals("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6"))
                .findFirst()
                .orElse(null);

        assertNotNull(consumeKafkaExtension);
        assertEquals(ExtensionType.PROCESSOR, consumeKafkaExtension.getType());

        final List<Property> properties = consumeKafkaExtension.getProperties();
        assertNotNull(properties);

        final Property maxUncommittedOffsetWaitProperty = properties.stream()
                .filter(p -> p.getName().equals("max-uncommit-offset-wait"))
                .findFirst()
                .orElse(null);
        assertNotNull(maxUncommittedOffsetWaitProperty);

        final List<Dependency> dependencies = maxUncommittedOffsetWaitProperty.getDependencies();
        assertNotNull(dependencies);
        assertEquals(1, dependencies.size());

        final Dependency dependency = dependencies.get(0);
        assertEquals("Commit Offsets", dependency.getPropertyName());
        assertEquals("Commit Offsets", dependency.getPropertyDisplayName());

        final DependentValues dependentValues = dependency.getDependentValues();
        assertNotNull(dependentValues);

        final List<String> dependentValuesList = dependentValues.getValues();
        assertNotNull(dependentValuesList);
        assertEquals(1, dependentValuesList.size());
        assertEquals("true", dependentValuesList.get(0));
    }

    @Test
    public void testDocsWithResourceDefinitions() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-kafka-2-6-nar.xml");
        assertNotNull(extensionManifest);
        assertEquals("1.16.0-SNAPSHOT", extensionManifest.getSystemApiVersion());

        final List<Extension> extensionDetails = extensionManifest.getExtensions();

        final Extension consumeKafkaExtension = extensionDetails.stream()
                .filter(e -> e.getName().equals("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6"))
                .findFirst()
                .orElse(null);

        assertNotNull(consumeKafkaExtension);
        assertEquals(ExtensionType.PROCESSOR, consumeKafkaExtension.getType());

        final List<Property> properties = consumeKafkaExtension.getProperties();
        assertNotNull(properties);

        final Property keytabProperty = properties.stream()
                .filter(p -> p.getName().equals("sasl.kerberos.keytab"))
                .findFirst()
                .orElse(null);
        assertNotNull(keytabProperty);

        final ResourceDefinition resourceDefinition = keytabProperty.getResourceDefinition();
        assertNotNull(resourceDefinition);
        assertEquals(Cardinality.SINGLE, resourceDefinition.getCardinality());

        final List<ResourceType> resourceTypes = resourceDefinition.getResourceTypes();
        assertNotNull(resourceTypes);
        assertEquals(1, resourceTypes.size());
        assertEquals(ResourceType.FILE, resourceTypes.get(0));
    }

    @Test
    public void testBundleAndBuildInfo() throws IOException {
        final ExtensionManifest extensionManifest = parse("src/test/resources/manifests/extension-manifest-kafka-2-6-nar.xml");
        assertNotNull(extensionManifest);

        assertEquals("org.apache.nifi", extensionManifest.getGroupId());
        assertEquals("nifi-kafka-2-6-nar", extensionManifest.getArtifactId());
        assertEquals("1.16.0-SNAPSHOT", extensionManifest.getVersion());

        assertNotNull(extensionManifest.getParentNar());
        assertEquals("org.apache.nifi", extensionManifest.getParentNar().getGroupId());
        assertEquals("nifi-standard-services-api-nar", extensionManifest.getParentNar().getArtifactId());
        assertEquals("1.16.0-SNAPSHOT", extensionManifest.getParentNar().getVersion());

        assertNotNull(extensionManifest.getBuildInfo());
        assertEquals("nifi-1.15.0-RC3", extensionManifest.getBuildInfo().getTag());
        assertEquals("main", extensionManifest.getBuildInfo().getBranch());
        assertEquals("123", extensionManifest.getBuildInfo().getRevision());
        assertEquals("1.8.0_282", extensionManifest.getBuildInfo().getJdk());
        assertEquals("jsmith", extensionManifest.getBuildInfo().getBuiltBy());
        assertEquals("2021-11-29T15:18:55Z", extensionManifest.getBuildInfo().getTimestamp());

        assertEquals("1.16.0-SNAPSHOT", extensionManifest.getSystemApiVersion());
    }

    private ExtensionManifest parse(final String file) throws IOException {
        try (final InputStream inputStream = new FileInputStream(file)) {
            return parser.parse(inputStream);
        }
    }
}
