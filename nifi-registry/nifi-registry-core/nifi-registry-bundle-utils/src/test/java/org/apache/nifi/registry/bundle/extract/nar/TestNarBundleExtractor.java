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
package org.apache.nifi.registry.bundle.extract.nar;

import org.apache.nifi.registry.bundle.extract.BundleException;
import org.apache.nifi.registry.bundle.extract.BundleExtractor;
import org.apache.nifi.registry.bundle.model.BundleIdentifier;
import org.apache.nifi.registry.bundle.model.BundleDetails;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNarBundleExtractor {

    private BundleExtractor extractor;

    @Before
    public void setup() {
        this.extractor = new NarBundleExtractor();
    }

    @Test
    public void testExtractFromGoodNarNoDependencies() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-framework-nar.nar")) {
            final BundleDetails bundleDetails = extractor.extract(in);
            assertNotNull(bundleDetails);
            assertNotNull(bundleDetails.getBundleIdentifier());
            assertNotNull(bundleDetails.getDependencies());
            assertEquals(0, bundleDetails.getDependencies().size());

            final BundleIdentifier bundleIdentifier = bundleDetails.getBundleIdentifier();
            assertEquals("org.apache.nifi", bundleIdentifier.getGroupId());
            assertEquals("nifi-framework-nar", bundleIdentifier.getArtifactId());
            assertEquals("1.8.0", bundleIdentifier.getVersion());

            assertNotNull(bundleDetails.getExtensions());
            assertEquals(0, bundleDetails.getExtensions().size());
            assertEquals("1.8.0", bundleDetails.getSystemApiVersion());
        }
    }

    @Test
    public void testExtractFromGoodNarWithDependencies() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-foo-nar.nar")) {
            final BundleDetails bundleDetails = extractor.extract(in);
            assertNotNull(bundleDetails);
            assertNotNull(bundleDetails.getBundleIdentifier());
            assertNotNull(bundleDetails.getDependencies());
            assertEquals(1, bundleDetails.getDependencies().size());

            final BundleIdentifier bundleIdentifier = bundleDetails.getBundleIdentifier();
            assertEquals("org.apache.nifi", bundleIdentifier.getGroupId());
            assertEquals("nifi-foo-nar", bundleIdentifier.getArtifactId());
            assertEquals("1.8.0", bundleIdentifier.getVersion());

            final BundleIdentifier dependencyCoordinate = bundleDetails.getDependencies().stream().findFirst().get();
            assertEquals("org.apache.nifi", dependencyCoordinate.getGroupId());
            assertEquals("nifi-bar-nar", dependencyCoordinate.getArtifactId());
            assertEquals("2.0.0", dependencyCoordinate.getVersion());

            final Map<String,String> additionalDetails = bundleDetails.getAdditionalDetails();
            assertNotNull(additionalDetails);
            assertEquals(0, additionalDetails.size());
        }
    }

    @Test(expected = BundleException.class)
    public void testExtractFromNarMissingRequiredManifestEntries() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-missing-manifest-entries.nar")) {
            extractor.extract(in);
            fail("Should have thrown exception");
        }
    }

    @Test(expected = BundleException.class)
    public void testExtractFromNarMissingManifest() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-missing-manifest.nar")) {
            extractor.extract(in);
            fail("Should have thrown exception");
        }
    }

    @Test(expected = BundleException.class)
    public void testExtractFromNarMissingExtensionDescriptor() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-foo-nar-missing-extension-descriptor.nar")) {
            extractor.extract(in);
            fail("Should have thrown exception");
        }
    }

    @Test
    public void testExtractFromNarWithDescriptorAndAdditionalDetails() throws IOException {
        try (final InputStream in = new FileInputStream("src/test/resources/nars/nifi-hadoop-nar.nar")) {
            final BundleDetails bundleDetails = extractor.extract(in);
            assertNotNull(bundleDetails);
            assertNotNull(bundleDetails.getBundleIdentifier());
            assertNotNull(bundleDetails.getDependencies());
            assertEquals(1, bundleDetails.getDependencies().size());

            final BundleIdentifier bundleIdentifier = bundleDetails.getBundleIdentifier();
            assertEquals("org.apache.nifi", bundleIdentifier.getGroupId());
            assertEquals("nifi-hadoop-nar", bundleIdentifier.getArtifactId());
            assertEquals("1.9.0-SNAPSHOT", bundleIdentifier.getVersion());

            final BuildInfo buildDetails = bundleDetails.getBuildInfo();
            assertNotNull(buildDetails);
            assertEquals("1.8.0_162", buildDetails.getBuildTool());
            assertEquals(NarBundleExtractor.NA, buildDetails.getBuildFlags());
            assertEquals("master", buildDetails.getBuildBranch());
            assertEquals("HEAD", buildDetails.getBuildTag());
            assertEquals("1a937b6", buildDetails.getBuildRevision());
            assertEquals("jsmith", buildDetails.getBuiltBy());
            assertNotNull(buildDetails.getBuilt());

            assertEquals("1.10.0-SNAPSHOT", bundleDetails.getSystemApiVersion());
            assertNotNull(bundleDetails.getExtensions());
            assertEquals(10, bundleDetails.getExtensions().size());

            final Map<String,String> additionalDetails = bundleDetails.getAdditionalDetails();
            assertNotNull(additionalDetails);
            assertEquals(3, additionalDetails.size());

            final String listHdfsKey = "org.apache.nifi.processors.hadoop.ListHDFS";
            assertTrue(additionalDetails.containsKey(listHdfsKey));
            assertTrue(additionalDetails.get(listHdfsKey).startsWith("<!DOCTYPE html>"));
            assertTrue(additionalDetails.get(listHdfsKey).trim().endsWith("</html>"));
        }
    }

}
