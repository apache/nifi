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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.processor.Processor;

import java.io.File;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockExtensionDiscoveringManager extends StandardExtensionDiscoveringManager {
    private final ConcurrentMap<String, ClassLoader> mockComponentClassLoaders = new ConcurrentHashMap<>();

    public synchronized void addProcessor(final Class<? extends Processor> mockProcessorClass) {
        final BundleDetails bundleDetails = new BundleDetails.Builder()
            .workingDir(new File("target/work/extensions/mock-bundle"))
            .coordinate(new BundleCoordinate("org.apache.nifi.mock", mockProcessorClass.getName(), "1.0.0"))
            .build();

        final Bundle mockBundle = new Bundle(bundleDetails, mockProcessorClass.getClassLoader());
        discoverExtensions(Set.of(mockBundle));

        mockComponentClassLoaders.put(mockProcessorClass.getName(), mockProcessorClass.getClassLoader());
        registerExtensionClass(Processor.class, mockProcessorClass.getName(), mockBundle);
    }

    public synchronized void addControllerService(final Class<? extends ControllerService> mockControllerServiceClass) {
        final BundleDetails bundleDetails = new BundleDetails.Builder()
            .workingDir(new File("target/work/extensions/mock-bundle"))
            .coordinate(new BundleCoordinate("org.apache.nifi.mock", mockControllerServiceClass.getName(), "1.0.0"))
            .build();

        final Bundle mockBundle = new Bundle(bundleDetails, mockControllerServiceClass.getClassLoader());
        discoverExtensions(Set.of(mockBundle));

        mockComponentClassLoaders.put(mockControllerServiceClass.getName(), mockControllerServiceClass.getClassLoader());
        registerExtensionClass(ControllerService.class, mockControllerServiceClass.getName(), mockBundle);
    }

    @Override
    public InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls,
            final boolean register, final String classloaderIsolationKey) {

        final ClassLoader classLoader = mockComponentClassLoaders.get(classType);
        if (classLoader != null) {
            return new InstanceClassLoader(instanceIdentifier, classType, additionalUrls, Set.of(), classLoader);
        }

        return super.createInstanceClassLoader(classType, instanceIdentifier, bundle, additionalUrls, register, classloaderIsolationKey);
    }
}
