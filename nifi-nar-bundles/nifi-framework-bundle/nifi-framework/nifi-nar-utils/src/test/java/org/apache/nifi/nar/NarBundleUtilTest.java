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
package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NarBundleUtilTest {

    @Test
    public void testManifestWithVersioningAndBuildInfo() throws IOException {
        final File narDir = new File("src/test/resources/nars/nar-with-versioning");
        final BundleDetails narDetails = NarBundleUtil.fromNarDirectory(narDir);
        assertEquals(narDir.getPath(), narDetails.getWorkingDirectory().getPath());

        assertEquals("org.apache.nifi", narDetails.getCoordinate().getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getCoordinate().getId());
        assertEquals("1.2.0", narDetails.getCoordinate().getVersion());

        assertEquals("org.apache.nifi.hadoop", narDetails.getDependencyCoordinate().getGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getDependencyCoordinate().getId());
        assertEquals("1.2.1", narDetails.getDependencyCoordinate().getVersion());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutVersioningAndBuildInfo() throws IOException {
        final File narDir = new File("src/test/resources/nars/nar-without-versioning");
        final BundleDetails narDetails = NarBundleUtil.fromNarDirectory(narDir);
        assertEquals(narDir.getPath(), narDetails.getWorkingDirectory().getPath());

        assertEquals(BundleCoordinate.DEFAULT_GROUP, narDetails.getCoordinate().getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getCoordinate().getId());
        assertEquals(BundleCoordinate.DEFAULT_VERSION, narDetails.getCoordinate().getVersion());

        assertEquals(BundleCoordinate.DEFAULT_GROUP, narDetails.getDependencyCoordinate().getGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getDependencyCoordinate().getId());
        assertEquals(BundleCoordinate.DEFAULT_VERSION, narDetails.getDependencyCoordinate().getVersion());

        assertNull(narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertNull(narDetails.getBuildRevision());
        assertNull(narDetails.getBuildTag());
        assertNull(narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutNarDependency() throws IOException {
        final File narDir = new File("src/test/resources/nars/nar-without-dependency");
        final BundleDetails narDetails = NarBundleUtil.fromNarDirectory(narDir);
        assertEquals(narDir.getPath(), narDetails.getWorkingDirectory().getPath());

        assertEquals("org.apache.nifi", narDetails.getCoordinate().getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getCoordinate().getId());
        assertEquals("1.2.0", narDetails.getCoordinate().getVersion());

        assertNull(narDetails.getDependencyCoordinate());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test(expected = IOException.class)
    public void testFromManifestWhenNarDirectoryDoesNotExist() throws IOException {
        final File manifest = new File("src/test/resources/nars/nar-does-not-exist");
        NarBundleUtil.fromNarDirectory(manifest);
    }

}
