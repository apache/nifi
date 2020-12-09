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
import org.apache.nifi.util.StringUtils;

import java.io.File;

/**
 * Utility to create the system bundle.
 */
public final class SystemBundle {

    public static final BundleCoordinate SYSTEM_BUNDLE_COORDINATE = new BundleCoordinate(
            BundleCoordinate.DEFAULT_GROUP, "system", BundleCoordinate.DEFAULT_VERSION);

    /**
     * Returns a bundle representing the system class loader.
     *
     * @param niFiProperties a NiFiProperties instance which will be used to obtain the default NAR library path,
     *                       which will become the working directory of the returned bundle
     * @return a bundle for the system class loader
     */
    public static Bundle create(final NiFiProperties niFiProperties) {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

        final String narLibraryDirectory = niFiProperties.getProperty(NiFiProperties.NAR_LIBRARY_DIRECTORY);
        if (StringUtils.isBlank(narLibraryDirectory)) {
            throw new IllegalStateException("Unable to create system bundle because " + NiFiProperties.NAR_LIBRARY_DIRECTORY + " was null or empty");
        }

        final BundleDetails systemBundleDetails = new BundleDetails.Builder()
                .workingDir(new File(narLibraryDirectory))
                .coordinate(SYSTEM_BUNDLE_COORDINATE)
                .build();

        return new Bundle(systemBundleDetails, systemClassLoader);
    }
}
