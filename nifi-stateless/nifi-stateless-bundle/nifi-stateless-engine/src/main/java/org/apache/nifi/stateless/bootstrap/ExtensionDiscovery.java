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
package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.stateless.parameter.ParameterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExtensionDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ExtensionDiscovery.class);

    public static ExtensionDiscoveringManager discover(final File narWorkingDirectory, final ClassLoader systemClassLoader, final NarClassLoaders narClassLoaders) throws IOException {
        logger.info("Initializing NAR ClassLoaders");

        try {
            narClassLoaders.init(systemClassLoader, null, narWorkingDirectory);
        } catch (final ClassNotFoundException cnfe) {
            throw new IOException("Could not initialize Class Loaders", cnfe);
        }

        final Set<Bundle> narBundles = narClassLoaders.getBundles();

        final long discoveryStart = System.nanoTime();
        final StandardExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager(Collections.singleton(ParameterProvider.class));
        extensionManager.discoverExtensions(narBundles);

        final long discoveryMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - discoveryStart);
        logger.info("Successfully discovered extensions in {} milliseconds", discoveryMillis);

        return extensionManager;
    }

}
