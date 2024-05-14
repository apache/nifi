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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;
import java.util.Objects;

public class PythonBundle {
    public static final String GROUP_ID = "org.apache.nifi";
    public static final String ARTIFACT_ID = "python-extensions";
    public static final String VERSION = BundleCoordinate.DEFAULT_VERSION;

    public static final BundleCoordinate PYTHON_BUNDLE_COORDINATE = new BundleCoordinate(GROUP_ID, ARTIFACT_ID, VERSION);

    public static Bundle create(final NiFiProperties properties, final ClassLoader classLoader) {
        final File pythonWorkingDirectory = new File(properties.getProperty(NiFiProperties.PYTHON_WORKING_DIRECTORY, NiFiProperties.DEFAULT_PYTHON_WORKING_DIRECTORY));

        final BundleDetails systemBundleDetails = new BundleDetails.Builder()
            .workingDir(pythonWorkingDirectory)
            .coordinate(PYTHON_BUNDLE_COORDINATE)
            .build();

        return new Bundle(systemBundleDetails, classLoader);
    }

    public static boolean isPythonCoordinate(final BundleCoordinate coordinate) {
        return isPythonCoordinate(coordinate.getGroup(), coordinate.getId());
    }

    public static boolean isPythonCoordinate(final String groupId, final String artifactId) {
        return Objects.equals(PYTHON_BUNDLE_COORDINATE.getGroup(), groupId)
            && Objects.equals(PYTHON_BUNDLE_COORDINATE.getId(), artifactId);
    }
}
