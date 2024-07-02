/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class NarManifestTest {

    @Test
    public void testFromManifestFull() throws IOException {
        final String manifestPath = "nar/MANIFEST-FULL.MF";
        final Manifest manifest = loadManifest(manifestPath);
        final NarManifest narManifest = NarManifest.fromManifest(manifest);
        assertNotNull(narManifest);

        assertEquals("nifi-framework-nar", narManifest.getId());
        assertEquals("org.apache.nifi", narManifest.getGroup());
        assertEquals("2.0.0-SNAPSHOT", narManifest.getVersion());

        assertEquals("nifi-jetty-nar", narManifest.getDependencyId());
        assertEquals("org.apache.nifi", narManifest.getDependencyGroup());
        assertEquals("2.0.0-SNAPSHOT", narManifest.getDependencyVersion());

        assertEquals("HEAD", narManifest.getBuildTag());
        assertEquals("main", narManifest.getBuildBranch());
        assertEquals("1234", narManifest.getBuildRevision());
        assertEquals("2024-01-26T00:11:29Z", narManifest.getBuildTimestamp());

        assertEquals("Apache NiFi Nar Maven Plugin 2.0.0", narManifest.getCreatedBy());

        final BundleCoordinate expectedCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-framework-nar", "2.0.0-SNAPSHOT");
        assertEquals(expectedCoordinate, narManifest.getCoordinate());

        final BundleCoordinate expectedDependencyCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-jetty-nar", "2.0.0-SNAPSHOT");
        assertEquals(expectedDependencyCoordinate, narManifest.getDependencyCoordinate());
    }

    @Test
    public void testFromManifestMinimal() throws IOException {
        final String manifestPath = "nar/MANIFEST-MINIMAL.MF";
        final Manifest manifest = loadManifest(manifestPath);
        final NarManifest narManifest = NarManifest.fromManifest(manifest);
        assertNotNull(narManifest);

        assertEquals("nifi-framework-nar", narManifest.getId());
        assertEquals("org.apache.nifi", narManifest.getGroup());
        assertEquals("2.0.0-SNAPSHOT", narManifest.getVersion());

        assertNull(narManifest.getDependencyId());
        assertNull(narManifest.getDependencyGroup());
        assertNull(narManifest.getDependencyVersion());

        assertNull(narManifest.getBuildTag());
        assertNull(narManifest.getBuildBranch());
        assertNull(narManifest.getBuildRevision());
        assertNull(narManifest.getBuildTimestamp());

        assertEquals("Apache NiFi Nar Maven Plugin 2.0.0", narManifest.getCreatedBy());

        final BundleCoordinate expectedCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-framework-nar", "2.0.0-SNAPSHOT");
        assertEquals(expectedCoordinate, narManifest.getCoordinate());
        assertNull(narManifest.getDependencyCoordinate());
    }

    private Manifest loadManifest(final String manifestPath) throws IOException {
        final ClassLoader classLoader = getClass().getClassLoader();
        final URL manifestResource = classLoader.getResource(manifestPath);
        if (manifestResource == null) {
            throw new IllegalArgumentException("Unable to find manifest on classpath: " + manifestPath);
        }

        final File manifestFile = new File(manifestResource.getFile());
        try (final InputStream manifestInputStream = new FileInputStream(manifestFile)) {
            return new Manifest(manifestInputStream);
        }
    }
}
