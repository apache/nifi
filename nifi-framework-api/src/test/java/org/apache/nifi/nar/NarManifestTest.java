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

    public static final String NIFI_GROUP_ID = "org.apache.nifi";

    public static final String NIFI_FRAMEWORK_NAR_ID = "nifi-framework-nar";
    public static final String NIFI_FRAMEWORK_NAR_VERSION = "2.0.0-SNAPSHOT";

    public static final String NIFI_JETTY_NAR_ID = "nifi-jetty-nar";
    public static final String NIFI_JETTY_NAR_VERSION = "2.0.0-SNAPSHOT";

    public static final String BUILD_TAG = "HEAD";
    public static final String BUILD_BRANCH = "main";
    public static final String BUILD_REVISION = "1234";
    public static final String BUILD_TIMESTAMP = "2024-01-26T00:11:29Z";

    @Test
    public void testFromManifestFull() throws IOException {
        final String manifestPath = "nar/MANIFEST-FULL.MF";
        final Manifest manifest = loadManifest(manifestPath);
        final NarManifest narManifest = NarManifest.fromManifest(manifest);
        assertNotNull(narManifest);

        assertEquals(NIFI_FRAMEWORK_NAR_ID, narManifest.getId());
        assertEquals(NIFI_GROUP_ID, narManifest.getGroup());
        assertEquals(NIFI_FRAMEWORK_NAR_VERSION, narManifest.getVersion());

        assertEquals(NIFI_JETTY_NAR_ID, narManifest.getDependencyId());
        assertEquals(NIFI_GROUP_ID, narManifest.getDependencyGroup());
        assertEquals(NIFI_JETTY_NAR_VERSION, narManifest.getDependencyVersion());

        assertEquals(BUILD_TAG, narManifest.getBuildTag());
        assertEquals(BUILD_BRANCH, narManifest.getBuildBranch());
        assertEquals(BUILD_REVISION, narManifest.getBuildRevision());
        assertEquals(BUILD_TIMESTAMP, narManifest.getBuildTimestamp());

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

        assertEquals(NIFI_FRAMEWORK_NAR_ID, narManifest.getId());
        assertEquals(NIFI_GROUP_ID, narManifest.getGroup());
        assertEquals(NIFI_FRAMEWORK_NAR_VERSION, narManifest.getVersion());

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
