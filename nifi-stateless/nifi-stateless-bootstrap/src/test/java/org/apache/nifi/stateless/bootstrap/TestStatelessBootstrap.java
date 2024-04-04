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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestStatelessBootstrap {

    @Test
    public void testClassloaderLoadsJavaLangObject() throws IOException, ClassNotFoundException {
        final File narDirectory = new File("target");

        try (final URLClassLoader systemClassLoader = new URLClassLoader(new URL[0])) {
            final AllowListClassLoader allowListClassLoader = StatelessBootstrap.createExtensionRootClassLoader(narDirectory, systemClassLoader);
            final Class<?> objectClass = allowListClassLoader.loadClass("java.lang.Object", true);
            assertNotNull(objectClass);
        }
    }

    @Test
    public void testClassNotAllowed() throws IOException, ClassNotFoundException {
        // Specify a class that should be loaded by the system class loader
        final File classFile = new File("target/classes");
        final URL classUrl = classFile.toURI().toURL();
        final String classToLoad = "org.apache.nifi.stateless.bootstrap.RunStatelessFlow";

        // A directory for NARs, jars, etc. that are allowed by the AllowListClassLoader
        final File narDirectory = new File("target/generated-sources");

        // Create a URLClassLoader to use for the System ClassLoader. This will load the classes from the target/ directory.
        // Then create an AllowListClassLoader that will not allow these classes through.
        // Ensure that the classes are not allowed through, but that classes in the java.lang still are.
        try (final URLClassLoader systemClassLoader = new URLClassLoader(new URL[] {classUrl})) {
            final AllowListClassLoader allowListClassLoader = StatelessBootstrap.createExtensionRootClassLoader(narDirectory, systemClassLoader);

            final Class<?> classFromSystemLoader = systemClassLoader.loadClass(classToLoad);
            assertNotNull(classFromSystemLoader);

            allowListClassLoader.loadClass("java.util.logging.Logger", true);

            Assertions.assertThrows(Exception.class, () -> {
                allowListClassLoader.loadClass(classToLoad);
            });

            final Class<?> objectClass = allowListClassLoader.loadClass("java.lang.Object", true);
            assertNotNull(objectClass);
        }
    }
}
