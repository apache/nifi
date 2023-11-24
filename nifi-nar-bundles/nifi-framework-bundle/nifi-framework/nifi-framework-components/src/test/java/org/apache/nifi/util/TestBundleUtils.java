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
package org.apache.nifi.util;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestBundleUtils {

    private static final String PROCESSOR_TYPE = "MyProcessor";
    private static final String FRAMEWORK_VERSION = "5.0.0";

    private static final Bundle frameworkBundle = createBundle("framework-bundle", FRAMEWORK_VERSION);
    private static ExtensionManager extensionManager;


    @BeforeAll
    public static void setup() throws IOException, ClassNotFoundException {
        extensionManager = Mockito.mock(ExtensionManager.class);

        final NarClassLoaders narClassLoaders = NarClassLoadersHolder.getInstance();
        narClassLoaders.init(null, new File("target/extensions"));
    }

    @Test
    public void findOptionalBundleMatchingFramework() throws IOException, ClassNotFoundException {
        final Bundle frameworkVersionBundle = createBundle("my-bundle", FRAMEWORK_VERSION);
        final Bundle otherBundle = createBundle("my-bundle", "1.2.3");
        final List<Bundle> bundles = Arrays.asList(frameworkVersionBundle, otherBundle);
        when(extensionManager.getBundles(PROCESSOR_TYPE)).thenReturn(bundles);

        final Optional<BundleCoordinate> compatibleCoordinate = BundleUtils.findOptionalBundleForType(extensionManager, PROCESSOR_TYPE, frameworkBundle);
        assertTrue(compatibleCoordinate.isPresent());
        assertEquals(frameworkVersionBundle.getBundleDetails().getCoordinate(), compatibleCoordinate.get());
    }

    @Test
    public void findOptionalBundleNotMatchingFramework() throws IOException, ClassNotFoundException {
        final Bundle version3 = createBundle("my-bundle", "3.0.0");
        final Bundle otherBundle = createBundle("my-bundle", "1.2.3");
        final List<Bundle> bundles = Arrays.asList(version3, otherBundle);
        when(extensionManager.getBundles(PROCESSOR_TYPE)).thenReturn(bundles);

        final Optional<BundleCoordinate> compatibleCoordinate = BundleUtils.findOptionalBundleForType(extensionManager, PROCESSOR_TYPE, frameworkBundle);
        assertFalse(compatibleCoordinate.isPresent());
    }

    @Test
    public void testFindOptionalBundleOnlyOneBundle() throws IOException, ClassNotFoundException {
        final Bundle otherBundle = createBundle("my-bundle", "1.2.3");
        final List<Bundle> bundles = Collections.singletonList(otherBundle);
        when(extensionManager.getBundles(PROCESSOR_TYPE)).thenReturn(bundles);

        final Optional<BundleCoordinate> compatibleCoordinate = BundleUtils.findOptionalBundleForType(extensionManager, PROCESSOR_TYPE, frameworkBundle);
        assertTrue(compatibleCoordinate.isPresent());
        assertEquals(otherBundle.getBundleDetails().getCoordinate(), compatibleCoordinate.get());
    }

    private static Bundle createBundle(final String artifactId, final String version) {
        final BundleDetails bundleDetails = new BundleDetails.Builder()
            .coordinate(new BundleCoordinate("org.apache.nifi", artifactId, version))
            .workingDir(new File("target"))
            .build();

        return new Bundle(bundleDetails, TestBundleUtils.class.getClassLoader());
    }
}
