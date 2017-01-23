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
package org.apache.nifi.bundle;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.io.File;

public class BundleDetailsTest {

    @Test
    public void testBuilder() {
        final File workingDirectory = new File("src/test/resources");

        final BundleCoordinate coordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-nar", "1.0.0");
        final BundleCoordinate dependencyCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-libraries-nar", "1.0.0");

        final String buildTag = "HEAD";
        final String buildRevision = "1";
        final String buildBranch = "DEV";
        final String buildTimestamp = "2017-01-01 00:00:00";
        final String buildJdk = "JDK8";
        final String builtBy = "bbende";

        final BundleDetails bundleDetails = new BundleDetails.Builder()
                .workingDir(workingDirectory)
                .coordinate(coordinate)
                .dependencyCoordinate(dependencyCoordinate)
                .buildTag(buildTag)
                .buildRevision(buildRevision)
                .buildBranch(buildBranch)
                .buildTimestamp(buildTimestamp)
                .buildJdk(buildJdk)
                .builtBy(builtBy)
                .build();

        assertEquals(workingDirectory, bundleDetails.getWorkingDirectory());
        assertEquals(coordinate, bundleDetails.getCoordinate());
        assertEquals(dependencyCoordinate, bundleDetails.getDependencyCoordinate());
        assertEquals(buildTag, bundleDetails.getBuildTag());
        assertEquals(buildRevision, bundleDetails.getBuildRevision());
        assertEquals(buildBranch, bundleDetails.getBuildBranch());
        assertEquals(buildTimestamp, bundleDetails.getBuildTimestamp());
        assertEquals(buildJdk, bundleDetails.getBuildJdk());
        assertEquals(builtBy, bundleDetails.getBuiltBy());
    }

    @Test(expected = IllegalStateException.class)
    public void testWorkingDirRequired() {
        final BundleCoordinate coordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-nar", "1.0.0");
        final BundleCoordinate dependencyCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-libraries-nar", "1.0.0");

        final String buildTag = "HEAD";
        final String buildRevision = "1";
        final String buildBranch = "DEV";
        final String buildTimestamp = "2017-01-01 00:00:00";
        final String buildJdk = "JDK8";
        final String builtBy = "bbende";

        new BundleDetails.Builder()
                .workingDir(null)
                .coordinate(coordinate)
                .dependencyCoordinate(dependencyCoordinate)
                .buildTag(buildTag)
                .buildRevision(buildRevision)
                .buildBranch(buildBranch)
                .buildTimestamp(buildTimestamp)
                .buildJdk(buildJdk)
                .builtBy(builtBy)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testCoordinateRequired() {
        final File workingDirectory = new File("src/test/resources");
        final BundleCoordinate dependencyCoordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-libraries-nar", "1.0.0");

        final String buildTag = "HEAD";
        final String buildRevision = "1";
        final String buildBranch = "DEV";
        final String buildTimestamp = "2017-01-01 00:00:00";
        final String buildJdk = "JDK8";
        final String builtBy = "bbende";

        new BundleDetails.Builder()
                .workingDir(workingDirectory)
                .coordinate(null)
                .dependencyCoordinate(dependencyCoordinate)
                .buildTag(buildTag)
                .buildRevision(buildRevision)
                .buildBranch(buildBranch)
                .buildTimestamp(buildTimestamp)
                .buildJdk(buildJdk)
                .builtBy(builtBy)
                .build();
    }

    @Test
    public void testDependencyCoordinateCanBeNull() {
        final File workingDirectory = new File("src/test/resources");

        final BundleCoordinate coordinate = new BundleCoordinate("org.apache.nifi", "nifi-hadoop-nar", "1.0.0");
        final BundleCoordinate dependencyCoordinate = null;

        final String buildTag = "HEAD";
        final String buildRevision = "1";
        final String buildBranch = "DEV";
        final String buildTimestamp = "2017-01-01 00:00:00";
        final String buildJdk = "JDK8";
        final String builtBy = "bbende";

        final BundleDetails bundleDetails = new BundleDetails.Builder()
                .workingDir(workingDirectory)
                .coordinate(coordinate)
                .dependencyCoordinate(dependencyCoordinate)
                .buildTag(buildTag)
                .buildRevision(buildRevision)
                .buildBranch(buildBranch)
                .buildTimestamp(buildTimestamp)
                .buildJdk(buildJdk)
                .builtBy(builtBy)
                .build();

        assertEquals(workingDirectory, bundleDetails.getWorkingDirectory());
        assertEquals(coordinate, bundleDetails.getCoordinate());
        assertEquals(dependencyCoordinate, bundleDetails.getDependencyCoordinate());
        assertEquals(buildTag, bundleDetails.getBuildTag());
        assertEquals(buildRevision, bundleDetails.getBuildRevision());
        assertEquals(buildBranch, bundleDetails.getBuildBranch());
        assertEquals(buildTimestamp, bundleDetails.getBuildTimestamp());
        assertEquals(buildJdk, bundleDetails.getBuildJdk());
        assertEquals(builtBy, bundleDetails.getBuiltBy());
    }

}
